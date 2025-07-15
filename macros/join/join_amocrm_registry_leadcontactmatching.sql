{%- macro join_amocrm_registry_leadcontactmatching(
    sourcetype_name,
    pipeline_name,
    relations_dict,
    date_from,
    date_to,
    params,
    limit0=none
    ) -%}

{{ config(
    materialized='table',
    order_by=('__table_name'),
    on_schema_change='fail'
) }}
{%- if execute -%}
{%- set sourcetype_name = 'amocrm' -%}
{%- set pipeline_name = 'events' -%} {# изначально у нас нет справочника, создаём его из данных стрима leads пайплайны events #}
{%- set template_name = 'default' -%}
{%- set stream_name = 'leads' -%}
{%- set table_pattern = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name ~ '_'  ~ template_name ~  '_(?:[^_]+_)?' ~ stream_name ~ '$' -%}
{%- set relations = datacraft.get_relations_by_re(schema_pattern=target.schema, table_pattern=table_pattern) -%}  
{%- if not relations -%} 
    {{ exceptions.raise_compiler_error('No relations were found matching the pattern "' ~ table_pattern ~ '". 
    Please ensure that your source data follows the expected structure.') }}
{%- endif -%} 
{%- set source_table = '(' ~ dbt_utils.union_relations(relations) ~ ')' -%} 
{%- if not source_table -%} 
    {{ exceptions.raise_compiler_error('No source_table were found by pattern "' ~ table_pattern ~ '"') }}
{%- endif -%}

SELECT
    splitByChar('_', __table_name)[6] AS accountName,
    id as leadId, -- ID сделки
    JSONExtractString(arrayJoin(JSONExtractArrayRaw(_embedded, 'contacts')), 'id') as contactId, -- ID контакта
    JSONExtractString(arrayJoin(JSONExtractArrayRaw(_embedded, 'contacts')), 'is_main') as contactIsMain, -- Является ли контакт главным для сделки
    __emitted_at,
    toLowCardinality(__table_name) AS __table_name,
    toLowCardinality('LeadContactMatching') AS __link 
FROM {{ source_table }}
{% if limit0 %}
LIMIT 0
{%- endif -%}

{%- endif -%}
{% endmacro %}