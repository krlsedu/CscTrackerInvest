alter table user_stocks
    add investment_type_id bigint not null
        references investment_types;

alter table user_stocks_movements
    add investment_type_id bigint not null
        references investment_types;