{%- macro attr_join_to_attr_prepare_with_qid(
  params = none,
  funnel_name=none,
  limit0=none,
  metadata=project_metadata()
  ) -%}

{%- set model_name_parts = (override_target_model_name or this.name).split('_') -%}
{%- set funnel_name = model_name_parts[1] -%}
{%- set funnels = metadata['funnels'] -%}
{%- set attribution_models = metadata['attribution_models'] -%}
{%- set funnel_steps = funnels[funnel_name].steps -%}
{%- set steps = metadata['steps'] -%}
{%- set attribution_models_list = funnels[funnel_name].models -%}

{#-
    Настройка материализации данных.
    order_by=('qid', '__period_number', '__datetime', '__priority', '__id') 
    определяет порядок сортировки данных по идентификатору группы, номеру периода, дате, приоритету и идентификатору.
-#}
{{
    config(
        materialized='table',
        order_by=('qid', '__period_number', '__datetime', '__priority', '__id')
    )
}}

select 
    y.__period_number as __period_number, 
    y.__priority as __priority, 
    y.__step as __step,
    x.*,

{#-
    Вычисление ранга для каждой модели атрибуции, указанной в funnel.models
-#}
{%- for model_name in attribution_models_list -%}
    {%- set model_data = attribution_models[model_name] -%}
    CASE
        {% for priority in model_data.priorities %}
            {%- set counter = loop.index -%}
            WHEN {{ priority }} THEN {{ counter }}
        {% endfor %}
        ELSE 0
    END as {{ '__' ~ model_data.type ~ '_rank' }}{% if not loop.last %},{% endif %}
{% endfor %}
from {{ ref('attr_' ~ funnel_name ~ '_prepare_with_qid') }} AS x
join {{ ref('attr_' ~ funnel_name ~ '_calculate_period_number') }} AS y
    using (qid, __datetime, __link, __id)

{% endmacro %}