{%- macro join_ym_events(
    sourcetype_name,
    pipeline_name,
    relations_dict,
    date_from,
    date_to,
    params,
    limit0=none
    ) -%}

{{ config(
    materialized='incremental',
    order_by=('__date', '__table_name'),
    incremental_strategy='delete+insert',
    unique_key=['__date', '__table_name'],
    on_schema_change='fail'
) }}
{%- if execute -%}
{%- set sourcetype_name = 'ym' -%}
{%- set pipeline_name = 'events' -%}
{%- set template_name = 'default' -%}
{%- set stream_name = 'raw_data_visits' -%} {# но может быть и иное название стрима, например, data_visits #}

{%- set table_pattern = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name ~ '_' ~ template_name ~ '(_\d+)?_' ~ stream_name -%}
{# set table_pattern = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name ~ '_[^_]+_'  #}
{%- set relations = datacraft.get_relations_by_re(schema_pattern=target.schema, table_pattern=table_pattern) -%}  
{%- if not relations -%} 
    {{ exceptions.raise_compiler_error('No relations were found matching the pattern "' ~ table_pattern ~ '". 
    Please ensure that your source data follows the expected structure.') }}
{%- endif -%}
{%- set source_table = '(' ~ dbt_utils.union_relations(relations) ~ ')' -%} 
{%- if not source_table -%} 
    {{ exceptions.raise_compiler_error('No source_table were found by pattern "' ~ table_pattern ~ '"') }}
{%- endif -%}

{#- получаем список date_from:xxx[0], date_to:yyy[0] из union всех normalize таблиц -#}
  {% set min_max_date_dict = datacraft.get_min_max_date('normalize',sourcetype_name) %}                                                             
  {% if not min_max_date_dict %} 
      {{ exceptions.raise_compiler_error('No min_max_date_dict') }} 
  {% endif %}
  {% set date_from = min_max_date_dict.get('date_from')[0] %}
  {% if not date_from %} 
      {{ exceptions.raise_compiler_error('No date_from') }} 
  {% endif %}
  {% set date_to = min_max_date_dict.get('date_to')[0] %}
  {% if not date_to %} 
      {{ exceptions.raise_compiler_error('No date_to') }} 
  {% endif %}

WITH events AS (
SELECT * FROM {{ source_table }}
{%- if date_from and  date_to %} 
WHERE toDate(__date) BETWEEN '{{date_from}}' AND '{{date_to}}'
{%- endif -%}
)

SELECT  
    __date, 
    toLowCardinality(splitByChar('_', __table_name)[7]) AS accountName,
    __table_name,  
    ymsvisitID As visitId,
    ymsclientID AS clientId,
    toDateTime(ymsdateTime) as eventDateTime,
    extract(ymspurchaseCoupon, '\'([^\'\[\],]+)') AS promoCode,   
    'web' AS osName,
    ymsregionCity AS cityName,
    lower(ymsregionCity) AS cityCode,
    assumeNotNull(coalesce({{ datacraft.get_adsourcedirty('ymsUTMSource', 'ymsUTMMedium') }}, 
    multiIf(ymslastTrafficSource = 'ad', {{ datacraft.get_adsourcedirty('ymslastAdvEngine', 'ymslastTrafficSource') }},  
    ymslastTrafficSource = 'organic', {{ datacraft.get_adsourcedirty('ymslastSearchEngine', 'ymslastTrafficSource') }},  
    {{ datacraft.get_adsourcedirty('ymslastReferalSource', 'ymslastTrafficSource') }}), '')) AS adSourceDirty, 
    ymsUTMSource AS utmSource,
    ymsUTMMedium AS utmMedium,
    ymsUTMCampaign AS utmCampaign,
    ymsUTMTerm AS utmTerm,
    ymsUTMContent AS utmContent,
    ymspurchaseID AS transactionId,
    {{ datacraft.get_utmhash('__', ['ymsUTMCampaign', 'ymsUTMContent']) }} AS utmHash,
    1 AS sessions,
    toBool(has(JSONExtractArrayRaw(ymsgoalsID), '425023443')) AS formSubmitSessions, 
    toUInt32(ymspageViews) AS pageViews,
    __emitted_at,
    toLowCardinality('VisitStat') AS __link 
FROM events
{% if limit0 %}
LIMIT 0
{%- endif -%}

{%-endif -%}
{% endmacro %}