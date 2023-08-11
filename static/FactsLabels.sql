select information as information,
       min(weight) as weight
from user_invest_facts
where user_id = :user_id
group by information
