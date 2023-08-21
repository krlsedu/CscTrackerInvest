select distinct urm.name                                                                                                                     as label,
                to_char(urm.data_fim, :date_mask)                                                                                            as date_time,
                first_value(urm.:sorted_by)
                over (partition by urm.name, to_char(urm.data_ini, :date_mask), to_char(urm.data_fim, :date_mask) order by create_date desc) as value
from user_resume_values urm,
     stocks s,
     investment_types it
where s.ticker = urm.name
  and it.id = s.investment_type_id
  and urm.data_ini = :data_ini
  and urm.data_fim between :data_ini and :data_fim
  and urm.user_id = :user_id
  and (:tipo = 'ativo' or :tipo = 'all')
  and (urm.name in (:ticker) or :ticker = 'all')
  and (it.name = :invest_name or 'all' = :invest_name)
  and it.id <> 1001
union
select distinct urm.name                                                                                                                     as label,
                to_char(urm.data_fim, :date_mask)                                                                                            as date_time,
                first_value(urm.:sorted_by)
                over (partition by urm.name, to_char(urm.data_ini, :date_mask), to_char(urm.data_fim, :date_mask) order by create_date desc) as value
from user_resume_values urm,
     investment_types it
where it.name = urm.name
  and urm.data_ini = :data_ini
  and urm.data_fim between :data_ini and :data_fim
  and urm.user_id = :user_id
  and (:tipo = 'tipo' or :tipo = 'all')
  and (it.name = :invest_name or 'all' = :invest_name)
  and it.id <> 1001
union
select distinct urm.name                                                                                                                     as label,
                to_char(urm.data_fim, :date_mask)                                                                                            as date_time,
                first_value(urm.:sorted_by)
                over (partition by urm.name, to_char(urm.data_ini, :date_mask), to_char(urm.data_fim, :date_mask) order by create_date desc) as value
from user_resume_values urm
where urm.data_ini = :data_ini
  and urm.data_fim between :data_ini and :data_fim
  and urm.user_id = :user_id
  and urm.name in ('Carteira')
  and ('carteira' = :tipo or 'nenhum' <> :indice)
union
select distinct urm.name                                                                                                                     as label,
                to_char(urm.data_fim, :date_mask)                                                                                            as date_time,
                first_value(urm.:sorted_by)
                over (partition by urm.name, to_char(urm.data_ini, :date_mask), to_char(urm.data_fim, :date_mask) order by create_date desc) as value
from user_resume_values urm,
     stocks s,
     investment_types it
where s.name = urm.name
  and it.id = s.investment_type_id
  and it.id = 1001
  and (s.name = :indice or 'all' = :indice)
  and urm.data_ini = :data_ini
  and urm.data_fim between :data_ini
    and :data_fim
  and urm.user_id = :user_id
order by date_time, label asc;
