{%- macro attr_find_new_period(
  params = none,
  funnel_name=none,
  limit0=none,
  metadata=project_metadata()
  ) -%}

{%- set model_name_parts = (override_target_model_name or this.name).split('_') -%}
{%- set model_name = model_name_parts[1] -%}

{# 
    Настройка материализации данных.
    materialized='table' указывает, что данные будут материализованы в таблицу.
    order_by=('qid', '__datetime', '__link', '__id') 
    определяет порядок сортировки данных по идентификатору группы, дате, ссылке и идентификатору.
#}
{{
    config(
        materialized='table',
        order_by=('qid', '__datetime', '__link', '__id')
    )
}}

{# 
    Извлечение метаданных и шагов воронки для формирования списка шагов и их порядкового номера.
#}

{%- set funnels = metadata['funnels'] -%}
{%- set step_name_list = funnels[funnel_name].steps -%}
{%- set counter = [] -%}
{%- set steps = metadata['steps'] -%}

{%- set counter = [] -%}
{%- for step_name in step_name_list -%}
    {%- do counter.append(loop.index) -%}
{% endfor %}

{# Извлечение данных с описанием шагов. Отсюда получаем данные по линкам #}
{# {% set steps =  events_description %} #}

{# 
    Подготовка нового периода.
    Для каждой группы определяется дата последнего события на шагах, входящих в состав периода.
#}
with prep_new_period as (
    select
        *,
        max(case when __priority in {{counter}} then __datetime else null end) over (partition by qid order by __rn rows between unbounded preceding and 1 preceding) as prep_new_period
    from {{ ref('attr_' ~ model_name ~ '_add_row_number') }}
)

{# 
    Определение, является ли каждое событие новым периодом.
    Для каждого события вычисляется разница между его датой и датой последнего события в периоде.
    Если разница меньше указанного периода (или 90 дней, если период не указан), 
    событие считается принадлежащим текущему периоду.
#}
select
    qid, 
    __link,
    __priority,
    __id,
    __datetime,
    __rn,
    __step,
    CASE
    {% for step_name in step_name_list %}
        {%- set counter = loop.index -%}
        WHEN __step = '{{step_name}}' and toDate(__datetime) - toDate(prep_new_period) < 
        {% for step_info in steps[step_name] %}
            {% if 'period' in step_info %} {{step_info.period}} {% else %} 90 {% endif %} THEN false
        {% endfor %}
    {%- endfor -%}
    ELSE true
END as __is_new_period
 from prep_new_period   
{% if limit0 %}
LIMIT 0
{%- endif -%}

{% endmacro %}