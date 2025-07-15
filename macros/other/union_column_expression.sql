{% macro union_column_expression(col_type, col_name) %}
    {%- if col_name is not none -%}
        {{ adapter.quote(col_name) }}
    {%- elif 'array' in col_type | lower -%}  {# Проверка на массив #}
        array()  {# Возвращаем пустой массив для типа данных array #}
    {%- elif 'String' in col_type -%}
        ''
    {%- else -%}
        0
    {%- endif -%}
{% endmacro %}