with resumo as (with meus_investimentos_com_dividendos_e_lucrosperdas as (with meus_investimentos_bruto
                                                                                   as (WITH ultima_cotacao
                                                                                                AS (SELECT DISTINCT ON (investment_id) investment_id,
                                                                                                                                       price
                                                                                                    FROM stocks_prices_agregated
                                                                                                    ORDER BY investment_id,
                                                                                                             date_value
                                                                                                        DESC)
                                                                                       SELECT grupo.macro_name as grupo_segmento,
                                                                                              round(us.quantity * COALESCE(uc.price, s.price), 2) as valor_total,
                                                                                              round(us.quantity * us.avg_price, 2)                as valor_investido,
                                                                                              grupo.ideal_perc_max as percentual_ideal_grupo,
                                                                                              usc.ideal_prec_max as percentual_ideal_segmento,
                                                                                              s.id,
                                                                                              s.ticker,
                                                                                              us.user_id,
                                                                                              it.name as tipo_investimento,
                                                                                              s.segment_custom as segmento
                                                                                       FROM user_macro_segments grupo
                                                                                                JOIN user_macro_segments_mapping umsm ON grupo.id = umsm.macro_segment_id
                                                                                                JOIN user_segments_configs usc ON umsm.segment_id = usc.id
                                                                                                JOIN stocks s ON s.segment_custom = usc.segment_name
                                                                                                JOIN user_stocks us ON us.investment_id = s.id
                                                                                                join investment_types it on it.id = s.investment_type_id
                                                                                                LEFT JOIN ultima_cotacao uc ON uc.investment_id = s.id)
                                                                          SELECT *,
                                                                                 coalesce(
                                                                                         (select sum(d.quantity * d.value_per_quote)
                                                                                          from dividends d
                                                                                          where d.investment_id = mib.id
                                                                                            and d.user_id = mib.user_id),
                                                                                         0)      as dividendos,
                                                                                 coalesce(
                                                                                         (select sum(pl.quantity * pl.value)
                                                                                          from profit_loss pl
                                                                                          where pl.investment_id = mib.id
                                                                                            and pl.user_id = mib.user_id),
                                                                                         0)      as lucrosperda
                                                                          FROM meus_investimentos_bruto mib)
                select grupo_segmento,
                       segmento,
                       ticker,
                       tipo_investimento,
                       percentual_ideal_grupo,
                       percentual_ideal_segmento,
                       sum(valor_total)             as valor_total,
                       sum(valor_investido)         as valor_aplicado,
                       coalesce(sum(dividendos), 0) as dividendos,
                       sum(lucrosperda)             as lucrosperda
                from meus_investimentos_com_dividendos_e_lucrosperdas micd
                group by grupo_segmento, segmento, ticker, tipo_investimento, percentual_ideal_grupo, percentual_ideal_segmento)
select
    grupo_segmento,
--     tipo_investimento,
--     'Geral' as tudo,
--     ticker,
    sum(valor_total) as total_hoje,

    -- 3. Rentabilidade Real Total (%) -> A métrica verdadeira sem bugar a matemática
    round(
        (((sum(valor_total) + sum(dividendos) + sum(lucrosperda)) / NULLIF(sum(valor_aplicado), 0)) - 1) * 100,
    2) as rentabilidade_real_perc,

    sum(valor_aplicado) as total_aplicado,

    -- O que chamávamos de investido real, vamos chamar de "Capital em Risco".
    -- Se for negativo, tu já tirou teu dinheiro e tá no lucro puro.
    round(sum(valor_aplicado - dividendos - lucrosperda), 2) as capital_em_risco,

    round(sum(dividendos),2) as dividendos,
    round(sum(lucrosperda),2) as lucrosperda,

    -- 1. Lucro só da variação da cota da tela da corretora (%)
    round(
        ((sum(valor_total) / NULLIF(sum(valor_aplicado), 0)) - 1) * 100,
    2) as lucro_cota_perc,

    -- 2. Lucro Financeiro Total em Dinheiro (R$) -> O que importa pro bolso
    round(
        (sum(valor_total) - sum(valor_aplicado)) + sum(dividendos) + sum(lucrosperda),
    2) as lucro_total_reais

from resumo
group by 1
order by rentabilidade_real_perc desc ;
