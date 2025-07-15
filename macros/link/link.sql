{%- macro link(
  params = none,
  disable_incremental=none,
  override_target_model_name=none,
  date_from = none,
  date_to = none,
  limit0=none
  ) 
-%}


{#- задаём части имени - чтобы выделить имя нужной hash-таблицы -#}
{%- set model_name_parts = (override_target_model_name or this.name).split('_') -%}
{%- set hash_raw_name = model_name_parts[1:] -%}
{%- set hash_name = '_'.join(hash_raw_name) -%}
{% set source_model_name = 'hash_' ~ hash_name %}

{#- задаём пустой список: сюда будем добавлять колонки, по которым будем делать GROUP BY -#}
{% set group_by_fields = [] %}

{#- задаём по возможности инкрементальность -#}

{%- set columns_names_with_date_type = [] -%}
{%- set source_columns = adapter.get_columns_in_relation(load_relation(ref(source_model_name))) -%}
{%- for c in source_columns -%}
{%- if 'Date' in c.data_type or 'DateTime' in c.data_type -%}
{%- do columns_names_with_date_type.append(c.name)  -%}
{%- endif -%}
{%- endfor -%} 
{%- if '__date' in columns_names_with_date_type -%}

{{ config(
    materialized='incremental',
    order_by=('__date', '__table_name'),
    incremental_strategy='delete+insert',
    unique_key=['__date', '__table_name'],
    on_schema_change='fail'
) }}

{#- если не установлено - будем делать table -#}
{%- else -%}

{{ config(
    materialized='table'
) }}

{%- endif -%}

{#- задаём наименования числовых типов данных -#}
{%- set numeric_types = ['UInt8', 'UInt16', 'UInt32', 'UInt64', 'UInt256', 
                        'Int8', 'Int16', 'Int32', 'Int64', 'Int128', 'Int256',
                        'Float8', 'Float16','Float32', 'Float64','Float128', 'Float256','Num'] -%}

{#- определяем колонки для группировки -#}
{%- set group_by_columns = ['__id', '__datetime'] -%}

SELECT 
{% for c in source_columns -%}
    {#- если колонка входит в список для группировки - выводим как есть #}
    {%- if c.name in group_by_columns -%}
        {{ c.name }}
    {#- если тип данных колонки числовой - суммируем #}
    {%- elif c.data_type in numeric_types -%}
        SUM({{ c.name }}) AS {{ c.name }}
    {#- для всех остальных колонок используем MAX #}
    {%- else -%}
        MAX({{ c.name }}) AS {{ c.name }}
    {%- endif -%}
    {%- if not loop.last %},{% endif %}
{% endfor %} 
FROM {{ ref(source_model_name) }}
GROUP BY {{ group_by_columns | join(', ') }}
{% if limit0 %}
LIMIT 0
{%- endif -%}
{% endmacro %}