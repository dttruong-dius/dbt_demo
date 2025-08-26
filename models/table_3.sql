-- table_3: depends on table_1 and table_2
select
    t1.id,
    t1.col1,
    t2.col2
from {{ ref('table_1') }} t1
join {{ ref('table_2') }} t2
    on t1.id = t2.id
