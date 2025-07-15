{%- macro custom_union_relations(relations, column_override=none, include=[], exclude=[], source_column_name='_dbt_source_relation') -%}

    {%- if exclude and include -%}
        {{ exceptions.raise_compiler_error("Both an exclude and include list were provided to the `union` macro. Only one is allowed") }}
    {%- endif -%}

    {#- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. -#}
    {%- if not execute %}
        {{ return('') }}
    {% endif -%}

    {%- if not relations -%}
        {{ exceptions.raise_compiler_error('There are no relations in macro custom_union_relations') }}
    {%- endif -%}

    {%- set column_override = column_override if column_override is not none else {} -%}

    {%- set relation_columns = {} -%}
    {%- set column_superset = {} -%}

    {%- for relation in relations -%}

        {%- do relation_columns.update({relation: []}) -%}

        {%- do dbt_utils._is_relation(relation, 'union_relations') -%}
        {%- do dbt_utils._is_ephemeral(relation, 'union_relations') -%}
        {%- set cols = adapter.get_columns_in_relation(relation) -%}
        {%- for col in cols -%}

            {#- If an exclude list was provided and the column is in the list, do nothing -#}
            {%- if exclude and col.column in exclude -%}

            {#- If an include list was provided and the column is not in the list, do nothing -#}
            {%- elif include and col.column not in include -%}

            {#- Otherwise add the column to the column superset -#}
            {%- else -%}

                {#- update the list of columns in this relation -#}
                {%- do relation_columns[relation].append(col.column) -%}

                {%- if col.column in column_superset -%}

                    {%- set stored = column_superset[col.column] -%}
                    {%- if col.is_string() and stored.is_string() and col.string_size() > stored.string_size() -%}
                        {%- do column_superset.update({col.column: col}) -%}
                    {%- endif %}

                {%- else -%}

                    {%- do column_superset.update({col.column: col}) -%}

                {%- endif -%}

            {%- endif -%}

        {%- endfor -%}
    {%- endfor -%}

    {%- set ordered_column_names = column_superset.keys() -%}

    {%- for relation in relations %}
(
SELECT {# яхз, это нужно для пробела #}
{%- for col_name in ordered_column_names -%}
    {%- set col = column_superset[col_name] %}
    {%- set col_type = column_override.get(col.column, col.data_type) %}
    
    {#- Check if column exists in current relation -#}
    {%- if col_name in relation_columns[relation] -%}
        {%- set col_expr = adapter.quote(col_name) -%}
    {%- else -%}
        {#- Use the union_column_expression macro to get default value -#}
        {%- set col_expr = datacraft.union_column_expression(col_type, none) -%}
    {%- endif -%}
    
    {#- Apply type casting (except for arrays) -#}
    {%- if 'array' in col_type | lower -%}
        {{ col_expr }} as {{ col.name }}{% if not loop.last %},{% endif %}
    {%- else -%}
        to{{ col_type.split('(')[0] }}({{ col_expr }}) as {{ col.name }}{% if not loop.last %},{% endif %}
    {%- endif -%}
{%- endfor %}
FROM {{ relation }}
)
{% if not loop.last -%}
UNION ALL
{% endif -%}
{%- endfor -%}

{%- endmacro -%}