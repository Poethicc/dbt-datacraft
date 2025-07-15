{%- macro attr_add_row_number(
  params = none,
  funnel_name=none,
  limit0=none
  ) -%}

{%- set model_name_parts = (override_target_model_name or this.name).split('_') -%}
{%- set model_name = model_name_parts[1] -%}

{# 
    Настройка материализации данных.
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
    Добавление порядкового номера строки (__rn) для каждой группы (qid), 
    упорядоченной по дате, приоритету и идентификатору.
#}
select
    *,
    row_number() over (partition by qid order by __datetime, __priority, __id) AS __rn
from {{ ref('attr_' ~ model_name ~ '_create_events') }}
{% if limit0 %}
LIMIT 0
{%- endif -%}

{% endmacro %}