with indices as (select s.name                as name,
                        :data_fim::date - :data_ini::date  as days, 1 as quantity_ini,
                        coalesce((select sp_ini.price
                                  from stocks_prices_agregated sp_ini
                                  where sp_ini.investment_id = s.id
                                    and sp_ini.date_value <= :data_ini
                                  order by date_value desc
                                 limit 1), 0) as price_ini,
                        1                     as quantity_fim,
                        (select sp_fim.price
                         from stocks_prices_agregated sp_fim
                         where sp_fim.investment_id = s.id
                           and sp_fim.date_value <= :data_fim
                         order by date_value desc
                                                 limit 1) as price_fim,
                        coalesce((select sp_ini.price
                                  from stocks_prices_agregated sp_ini
                                  where sp_ini.investment_id = s.id
                                    and sp_ini.date_value <=
                                        (select min(date)
                                         from user_stocks_movements
                                         where movement_type = 1
                                           and user_id = :p_user_id)
                                  order by date_value desc
                                  limit 1), 0)             as price_ini_geral,
                        :data_fim:: date -
                        (select min(date)
                         from user_stocks_movements
                         where movement_type = 1
                           and user_id = :p_user_id)::date as days_geral
from stocks s, investment_types it
where it.id = s.investment_type_id
  and it.id = 1001
  and (s.name = :indice
   or 'all' = :indice)
  and 'nenhum' <> :indice)
    , indices_final as (
select (price_fim / price_ini - 1) * 100 as valorizacao_real
        , price_fim - price_ini as crescimento
        , (price_fim / price_ini_geral - 1) * 100 as crescimento_percentual_geral
        , price_fim - price_ini_geral as crescimento_geral
        , *
from indices)
select name                                                     as name,
       days                                                     as days,

       SIGN(valorizacao_real) *
       round((power(1 + ABS(valorizacao_real) / 100,
                    1.0 / NULLIF(days, 0)) - 1) * 100, 6)       As ganho_diario_medio,

       SIGN(valorizacao_real) *
       round((power(1 + ABS(valorizacao_real) / 100,
                    30.0 / NULLIF(days, 0)) - 1) * 100, 6)      As ganho_mensalizado_medio,

       SIGN(valorizacao_real) *
       round((power(1 + ABS(valorizacao_real) / 100,
                    365.0 / NULLIF(days, 0)) - 1) * 100, 6)     As ganho_anualizado_medio,
       valorizacao_real / 100                                   as valorizacao_real,
       price_ini                                                as valor_ini,
       0                                                        as dividendos,
       0                                                        as vendas,
       0                                                        as aportes_real,
       crescimento                                              as crescimento,
       price_fim                                                as valor_fim,
       days_geral                                               as days_geral,

       SIGN(crescimento_geral) *
       round((power(1 + ABS(crescimento_geral) / 100,
                    1.0 / NULLIF(days_geral, 0)) - 1) * 100, 6) As impacto_diario_geral,

       SIGN(crescimento_geral) *
       round((power(1 + ABS(crescimento_geral) / 100,
                    1.0 / NULLIF(days_geral, 0)) - 1) * 100, 6) As impacto_mensalizado_geral,

       SIGN(crescimento_geral) *
       round((power(1 + ABS(crescimento_geral) / 100,
                    1.0 / NULLIF(days_geral, 0)) - 1) * 100, 6) As impacto_anualizado_geral,
       crescimento_geral / 100                                  as crescimento_geral,
       0                                                        as aportes_final,
       0                                                        as capital_em_risco_final,
       0                                                        as dividendos_final,
       0                                                        as vendas_final,
       crescimento_percentual_geral                             as crescimento_percentual_geral
from indices_final
union all
select grupo_ativo                          as name,
       dias_ponderado_periodo               as days,
       crescimento_diario_periodo           as ganho_diario_medio,
       crescimento_mensalizado_periodo      as ganho_mensalizado_medio,
       crescimento_anualizado_periodo       as ganho_anualizado_medio,
       crescimento_percentual_periodo / 100 as valorizacao_real,
       valor_inicial                        as valor_ini,
       dividendos_periodo                   as dividendos,
       lucrosperdas_periodo                 as vendas,
       valor_investido_periodo              as aportes_real,
       crescimento_periodo                  as crescimento,
       valor_final                          as valor_fim,
       dias_ponderado                       as days_geral,
       impacto_diario_patrimonio            as impacto_diario_geral,
       impacto_mensalizado_patrimonio       as impacto_mensalizado_geral,
       impacto_anualizado_patrimonio        as impacto_anualizado_geral,
       crescimento_geral / 100              as crescimento_geral,
       valor_investido_final                as aportes_final,
       capital_em_risco_final               as capital_em_risco_final,
       dividendos_final                     as dividendos_final,
       lucrosperdas_final                   as vendas_final,
       crescimento_percentual_geral         as crescimento_percentual_geral
from fn_resumo_investimentos_agrupado(
        :data_ini,
        :data_fim,
        :p_user_id,
        :p_agrupamento,
        :p_ticker,
        :p_tipo_investimento,
        :p_segmento,
        :p_grupo_segmento
     )
order by impacto_anualizado_geral desc;