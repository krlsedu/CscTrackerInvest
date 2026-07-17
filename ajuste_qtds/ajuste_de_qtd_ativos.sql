
truncate table tickers_b3;

select *
from tickers_b3;

select b3.ticker, b3.quantidade, us.quantity
from tickers_b3 b3,
     stocks s,
     user_stocks us
where b3.ticker = s.ticker
  and s.id = us.investment_id
  and us.quantity <> b3.quantidade
order by us.quantity desc;

select s.ticker, b3.quantidade
from stocks s,
     tickers_b3 b3
where s.ticker = b3.ticker
  and s.investment_type_id < 15
  and b3.quantidade >= 1
  and not exists (select 1 from user_stocks us where s.id = us.investment_id);

select s.ticker, us.quantity
from stocks s,
     user_stocks us
where s.id = us.investment_id
  and s.investment_type_id < 15
  and us.quantity >= 1
  and not exists (select 1 from tickers_b3 b3 where b3.ticker = s.ticker);

select *
from user_stocks us,
     stocks s
where us.investment_id = s.id
 and s.ticker = 'OULG11'
order by s.id desc;

-- update user_stocks_movements set date = buy_date where date > '2025-11-28' and date <> buy_date;

-- select ajustar_desdobramento('RNEW3', 2, 1, '2025-06-03');

select *
from tickers_b3 b3
where not exists (select 1
                  from stocks s,
                       user_stocks us
                  where s.ticker = b3.ticker
                    and s.id = us.investment_id)
order by quantidade desc;
