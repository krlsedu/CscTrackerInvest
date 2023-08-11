select name                       as name,
       days                       as days,
       round(valorizacao_real, 4) as valorizacao_real,
       round(case
                 when days > 0 then power((valorizacao_real + 1), (1 / (days))) - 1
                 else power((valorizacao_real + 1), (1 / (real_days))) - 1 end * 100,
             6)                   as ganho_diario_medio,
       round(case
                 when days > 0 then power(power((valorizacao_real + 1), (1 / (days))), 30) - 1
                 else power(round(power((valorizacao_real + 1), (1 / (real_days))), 12), 30) - 1 end * 100,
             4)                   as ganho_mensalizado_medio,
       round(case
                 when days > 0 then power(power((valorizacao_real + 1), (1 / (days))), 365) - 1
                 else power(round(power((valorizacao_real + 1), (1 / (real_days))), 12), 365) - 1 end * 100,
             2)                   as ganho_anualizado_medio,
       *
from (select name,
             round(valor_ini, 2)                                                 as valor_ini,
             round(valor_fim, 2)                                                 as valor_fim,
             (:data_fim::date - :data_ini::date)::numeric                        as real_days,
             case when valor_fim <> 0 then round(days / valor_fim, 0) else 0 end as days,
             case
                 when ((aportes - dividendos) + valor_ini) <> 0
                     then (valor_fim / ((aportes - dividendos) + valor_ini)) - 1
                 else 0
                 end                                                             as valorizacao_real,
             round((valor_fim - ((aportes - dividendos) + valor_ini)), 2)        as crescimento_real,
             round(aportes, 2)                                                   as aportes,
             round(aportes - dividendos, 2)                                      as aportes_real,
             round((valor_fim - ((aportes) + valor_ini)), 2)                     as crescimento,
             round(dividendos, 2)                                                as dividendos,
             round(vendas, 2)                                                    as vendas,
             round(aplicacoes, 2)                                                as aplicacoes,
             round(resgates, 2)                                                  as resgates
      from (select name,
                   sum(quantity_ini * price_ini) as valor_ini,
                   sum(quantity_fim * price_fim) as valor_fim,
                   sum(aportes) - sum(resgates)  as aportes,
                   sum(aportes)                  as aplicacoes,
                   sum(resgates)                 as resgates,
                   sum(dividendos)               as dividendos,
                   sum(vendas)                   as vendas,
                   sum(days * price_fim)         as days
            from (select (select coalesce(
                                         sum(case when movement_type = 1 then quantity else -quantity end),
                                         0)
                          from user_stocks_movements usm_ini
                          where usm_ini.investment_id = s.id
                            and usm_ini.user_id = us.user_id
                            and usm_ini.date <= :data_ini)                as quantity_ini,
                         coalesce((select sp_ini.price
                                   from stocks_prices sp_ini
                                   where sp_ini.investment_id = s.id
                                     and sp_ini.date_value <= :data_ini
                                   order by date_value desc
                                   limit 1), 0)                           as price_ini,
                         (select coalesce(
                                         sum(case when movement_type = 1 then quantity else -quantity end),
                                         0)
                          from user_stocks_movements usm_fim
                          where usm_fim.investment_id = s.id
                            and usm_fim.user_id = us.user_id
                            and usm_fim.date <= :data_fim)                as quantity_fim,
                         (select sp_fim.price
                          from stocks_prices sp_fim
                          where sp_fim.investment_id = s.id
                            and sp_fim.date_value <= :data_fim
                          order by date_value desc
                          limit 1)                                        as price_fim,
                         (select coalesce(sum(quantity * usm_incres.price), 0)
                          from user_stocks_movements usm_incres
                          where usm_incres.investment_id = s.id
                            and usm_incres.user_id = us.user_id
                            and usm_incres.movement_type = 1
                            and usm_incres.date >= :data_ini
                            and usm_incres.date <= :data_fim)             as aportes,
                         (select coalesce(sum(quantity * usm_incres.price), 0)
                          from user_stocks_movements usm_incres
                          where usm_incres.investment_id = s.id
                            and usm_incres.user_id = us.user_id
                            and usm_incres.movement_type = 2
                            and usm_incres.date >= :data_ini
                            and usm_incres.date <= :data_fim)             as resgates,
                         coalesce((select sum(value_per_quote * d.quantity)
                                   from dividends d
                                   where d.investment_id = s.id
                                     and d.date_payment >= :data_ini
                                     and d.date_payment <= :data_fim), 0) as dividendos,
                         coalesce((select sum(pl.quantity * pl.value)
                                   from profit_loss pl
                                   where pl.investment_id = s.id
                                     and pl.date_sell >= :data_ini
                                     and pl.date_sell <= :data_fim), 0)   as vendas,
                         case
                             when :tipo = 'carteira' then 'Carteira'
                             when :tipo = 'tipo' then it.name
                             when :tipo = 'ativo' then s.ticker
                             else 'Carteira' end                          as name,

                         (select coalesce(sum(case
                                                  when movement_type = 1
                                                      then (:data_fim::date - case
                                                                                  when :data_ini > usm_ini.date
                                                                                      then :data_ini::date
                                                                                  else usm_ini.date::date end) *
                                                           usm_ini.quantity
                                                  else - (:data_fim::date - case
                                                                                when :data_ini > usm_ini.date
                                                                                    then :data_ini::date
                                                                                else usm_ini.date::date end) *
                                                       usm_ini.quantity end),
                                          0)
                          from user_stocks_movements usm_ini
                          where usm_ini.investment_id = s.id
                            and usm_ini.user_id = us.user_id
                            and usm_ini.date <= :data_fim)                as days
                  from user_stocks us,
                       stocks s,
                       investment_types it
                  where us.investment_id = s.id
                    and it.id = s.investment_type_id
                    and (it.name = :invest_name or 'all' = :invest_name)
                    and (s.ticker = :ticker or 'all' = :ticker)
                    and us.user_id = :user_id
                  union
                  select 1                      as quantity_ini,
                         coalesce((select sp_ini.price
                                   from stocks_prices sp_ini
                                   where sp_ini.investment_id = s.id
                                     and sp_ini.date_value <= :data_ini
                                   order by date_value desc
                                   limit 1), 0) as price_ini,
                         1                      as quantity_fim,
                         (select sp_fim.price
                          from stocks_prices sp_fim
                          where sp_fim.investment_id = s.id
                            and sp_fim.date_value <= :data_fim
                          order by date_value desc
                          limit 1)              as price_fim,
                         0                      as aportes,
                         0                      as resgates,
                         0                      as dividendos,
                         0                      as vendas,
                         s.name                 as name,
                         0                      as days
                  from stocks s,
                       investment_types it
                  where it.id = s.investment_type_id
                    and it.id = 1001
                    and (s.name = :indice or 'all' = :indice)) as t
            group by name) as t2) as t3
order by ganho_anualizado_medio desc;
