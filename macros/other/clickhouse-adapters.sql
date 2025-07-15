{% macro get_relations_by_re(schema_pattern, table_pattern, database=target.database) -%}
  {{ return(adapter.dispatch('get_relations_by_re', 'datacraft')(schema_pattern, table_pattern, database)) }}
{%- endmacro %}

{% macro get_tables_by_re_sql(schema_pattern, table_pattern, database=target.database) -%}
  {{ adapter.dispatch('get_tables_by_re_sql', 'datacraft')(schema_pattern, table_pattern, database) }}
{%- endmacro %}

{% macro clickhouse__get_tables_by_re_sql(schema_pattern, table_pattern, database=target.database) %}
        select distinct
            table_schema as {{ adapter.quote("table_schema") }},
            table_name as {{ adapter.quote("table_name") }},
            {{ datacraft.get_table_types_sql() }}
        from information_schema.tables
        where match(table_schema, '^{{ schema_pattern }}$')
        and match(table_name, '{{ table_pattern }}')        
{% endmacro %}

{% macro clickhouse__get_table_types_sql() %}
            case toString(table_type)
                when 'BASE TABLE' then 'table'
                when 'EXTERNAL TABLE' then 'external'
                when 'MATERIALIZED VIEW' then 'materializedview'
                else lower(toString(table_type))
            end as {{ adapter.quote('table_type') }}
{% endmacro %}

{% macro clickhouse__get_relations_by_prefix(schema, prefix, exclude='', database=target.database) %}
    {%- call statement('get_tables', fetch_result=True) %}
      {{ dbt_utils.get_tables_by_prefix_sql(schema, prefix, exclude, database) }}
    {%- endcall -%}
    {%- set table_list = load_result('get_tables') -%}
    {%- if table_list and table_list['table'] -%}
        {%- set tbl_relations = [] -%}
        {%- for row in table_list['table'] -%}
            {%- set tbl_relation = api.Relation.create(
                database=none,
                schema=row.table_schema,
                identifier=row.table_name,
                type=row.table_type
            ) -%}
            {%- do tbl_relations.append(tbl_relation) -%}
        {%- endfor -%}
        {{ return(tbl_relations) }}
    {%- else -%}
        {{ return(none) }}
    {%- endif -%}
{% endmacro %}

{% macro clickhouse__get_relations_by_re(schema_pattern, table_pattern, database=target.database) %}
    {%- call statement('get_tables', fetch_result=True) %}

      {{ datacraft.get_tables_by_re_sql(schema_pattern, table_pattern, database) }}

    {%- endcall -%}

    {%- set table_list = load_result('get_tables') -%}
    {%- if table_list and table_list['table'] -%}
        {%- set tbl_relations = [] -%}
        {%- for row in table_list['table'] -%}
            {%- set tbl_relation = api.Relation.create(
                database=none,
                schema=row.table_schema,
                identifier=row.table_name,
                type=row.table_type
            ) -%}
            {%- do tbl_relations.append(tbl_relation) -%}
        {%- endfor -%}
        {{ return(tbl_relations) }}
    {%- else -%}
        {{ return(none) }}
    {%- endif -%}
{% endmacro %}

