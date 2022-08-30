alter table user_stocks
    add investment_type_id bigint not null
        references investment_types;

alter table user_stocks_movements
    add investment_type_id bigint not null
        references investment_types;

-- 2022-08-30
alter table user_stocks
    add buy_date timestamp;

alter table user_stocks
    add sell_date timestamp;

alter table user_stocks
    add end_date timestamp;


alter table stocks
    add ev_ebit numeric;