{%- macro attr_final_table(
  params = none,
  funnel_name=none,
  limit0=none
  ) -%}

{# 
    Настройка материализации данных.
    Порядок сортировки: по времени (__datetime).
#}
{{
    config(
        materialized = 'table',
        order_by = ('__datetime')
    )
}}

{# 
    Выборка данных для окончательной таблицы.
    Присоединение результатов двух предыдущих макросов: attr_join_to_attr_prepare_with_qid и attr_model.
#}
with 
    out as ( 
        select * except(_dbt_source_relation) 
        from  {{ ref('attr_' ~ funnel_name ~ '_join_to_attr_prepare_with_qid') }}
        join  {{ ref('attr_' ~ funnel_name ~ '_model') }}
            using (qid, __datetime, __id, __link, __period_number, __priority)
    )
    
select * from out 
{% if limit0 %}
LIMIT 0
{%- endif -%}

{%- endmacro %}
