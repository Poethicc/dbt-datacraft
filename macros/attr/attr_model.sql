{%- macro attr_model(
  params = none,
  funnel_name=none,
  limit0=none,
  metadata=project_metadata()
  ) -%}

{%- set funnels = metadata['funnels'] -%}
{%- set attribution_models = metadata['attribution_models'] -%}
{%- set model_list = funnels[funnel_name].models -%}

{{
    config(
        materialized='table',
        order_by = ('qid', '__datetime', '__id')
    )
}}

with max_click_rank as (
    select
        *
        {% for model_name in model_list %}
        {% set model_type = attribution_models[model_name]['type'] %}
        ,max({{'__'~ model_type ~ '_rank' }}) over(partition by qid, __period_number order by __datetime, __priority, __id) as {{'__max_' ~ model_type ~ '_rank' }}
        {% endfor %}
    from {{ ref('attr_' ~ funnel_name ~ '_join_to_attr_prepare_with_qid') }}
),

target_count as (
    select
        *
        {% for model_name in model_list %}
        {% set model_type = attribution_models[model_name]['type'] %}
        ,{{'__'~ model_type ~ '_rank' }} = {{'__max_' ~ model_type ~ '_rank' }} as {{'__' ~ model_type ~ '__rank_condition' }}
        ,sum(case when {{'__' ~ model_type ~ '__rank_condition' }} then 1 else 0 end) 
            over(partition by qid, __period_number order by __datetime, __priority, __id) as {{'__' ~ model_type ~ '__target_count' }}
        {% endfor %}
    from max_click_rank
)

SELECT 
    qid, __datetime, __id, __priority, __link, __period_number
    {%- for model_name in model_list -%}
    {%- set model_type = attribution_models[model_name]['type'] -%}
    {%- set fields = attribution_models[model_name]['fields'] -%}
    
    {# Last click attribution #}
    {% if model_type == 'last_click' %}
        {%- for field in fields -%}
            ,first_value({{field}}) over(
                partition by qid, __period_number, {{'__' ~ model_type ~ '__target_count' }}  
                order by __datetime, __priority, __id
            ) as {{'__' ~ funnel_name ~'_'~ model_type ~'_'~ field}}
        {% endfor %}
    
    {# First click attribution #}
    {% elif model_type == 'first_click' %}
        {%- for field in fields -%}
            ,first_value({{field}}) over(
                partition by qid, __period_number 
                order by {{'__' ~ model_type ~ '_rank' }} desc, __datetime, __priority, __id
            ) as {{'__' ~ funnel_name ~'_'~ model_type ~'_'~ field}}
        {% endfor %}
    {% endif %}
    {%- endfor -%}
FROM target_count
{% if limit0 %}
LIMIT 0
{%- endif -%}
{%- endmacro %}