{%- macro attr_create_missed_steps(
  params = none,
  funnel_name=none,
  limit0=none
  ) -%}

{# 
    Настройка материализации данных.
    order_by=('qid', '__datetime','__link','__id') 
    определяет порядок сортировки данных по идентификатору группы, дате, ссылке и идентификатору.
#}
{{
    config(
        materialized='table',
        order_by=('qid', '__datetime','__link','__id')
    )
}}

{# 
    Расчет максимального приоритета для каждой группы (qid) и периода.
#}
with calc_max_priority as (
    select
        qid, 
        __link,
        __id,
        __datetime,
        __rn,
        __priority,
        __period_number,
        __step,
        max(__priority) over(partition by qid, __period_number) as max_priority
    from {{ ref('attr_' ~ model_name ~ '_calculate_period_number') }}
),

{# 
    Генерация всех приоритетов для каждой группы и каждого периода.
#}
generate_all_priorities as (
    select
        distinct qid, __link,
        arrayJoin(range(1, assumeNotNull(max_priority) + 1)) as gen_priority
    from calc_max_priority
),

{# 
    Создание всех возможных комбинаций пропущенных шагов для каждой группы и каждого приоритета.
#}
final as (
    select
        first_value(__id) OVER (PARTITION BY qid ORDER BY gen_priority ROWS BETWEEN current row AND UNBOUNDED FOLLOWING ) as __id,
        gen_priority as __priority,
        qid, __link,
        first_value(__datetime) OVER (PARTITION BY qid ORDER BY gen_priority ROWS BETWEEN current row AND UNBOUNDED FOLLOWING ) as __datetime,
        first_value(__period_number) OVER (PARTITION BY qid ORDER BY gen_priority ROWS BETWEEN current row AND UNBOUNDED FOLLOWING ) as __period_number,
        case when calc_max_priority.qid = 0 then true else false end as __if_missed,
        __step
    from generate_all_priorities
    left join calc_max_priority
        on generate_all_priorities.qid = calc_max_priority.qid and
           generate_all_priorities.gen_priority = calc_max_priority.__priority
)

{# 
    Выборка данных с добавлением порядкового номера строки (__rn) для каждой группы.
#}
select
    qid, __link,
     __id,
    __priority,
     __datetime,
    __period_number,
    __if_missed,
    __step,
    row_number() over (partition by qid order by __datetime, __priority, __id) AS __rn
from final
{% if limit0 %}
LIMIT 0
{%- endif -%}

{% endmacro %}