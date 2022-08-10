insert into movement_types (name) values ('Buy');
insert into movement_types (name,coefficient) values ('Sell',-1);
insert into movement_types (name) values ('Transfer In');
insert into movement_types (name,coefficient) values ('Transfer Out',-1);
insert into movement_types (name) values ('Receive');
insert into movement_types (name) values ('Dividend');
insert into movement_types (name) values ('Rendiment');
insert into movement_types (name) values ('JCP');
insert into movement_types (name,coefficient) values ('Cancellation',-1);


insert into investment_types (id,name) values (1, 'Ação');
insert into investment_types (id,name) values (2, 'Fundo imobiliário');
insert into investment_types (id,name) values (3, 'Tesouro direto');
insert into investment_types (id,name) values (4, 'BDR');
insert into investment_types (id,name) values (6, 'ETF');
insert into investment_types (id,name) values (24, 'Fiagros');
insert into investment_types (id,name) values (15, 'Fundo de investimento');
insert into investment_types (id,name) values (16, 'CBD/LCI/LCA/LC/RDB');
insert into investment_types (id,name) values (12, 'Stocks');
insert into investment_types (id,name) values (13, 'Reits');
insert into investment_types (id,name) values (901, 'ETF - Exterior');
insert into investment_types (id,name) values (100, 'Criptomoedas');