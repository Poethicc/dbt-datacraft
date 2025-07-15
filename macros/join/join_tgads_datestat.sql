{%- macro join_tgads_datestat(
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
{%- set sourcetype_name = 'tgads' -%}
{%- set pipeline_name_datestat = 'datestat' -%} 
{%- set pipeline_name_registry = 'registry' -%}
{%- set template_name = 'default' -%}

{%- set stream_name_ads_statistics = 'ads_statistics' -%}
{%- set table_pattern_ads_statistics = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name_datestat ~ '_' ~ template_name ~  '_(?:[^_]+_)?' ~ stream_name_ads_statistics ~ '$' -%}
{%- set relations_ads_statistics = datacraft.get_relations_by_re(schema_pattern=target.schema, table_pattern=table_pattern_ads_statistics) -%}   
{%- if not relations_ads_statistics -%} 
    {{ exceptions.raise_compiler_error('No relations were found matching the pattern "' ~ table_pattern_ads_statistics ~ '". 
    Please ensure that your source data follows the expected structure.') }}
{%- endif -%} 
{%- set source_table_ads_statistics = '(' ~ dbt_utils.union_relations(relations_ads_statistics) ~ ')' -%} 
{%- if not source_table_ads_statistics -%} 
    {{ exceptions.raise_compiler_error('No source_table were found by pattern "' ~ table_pattern_ads_statistics ~ '"') }}
{%- endif -%} 

{%- set stream_name_ads_details = 'ads_details' -%}
{%- set table_pattern_ads_details = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name_registry ~ '_' ~ template_name ~  '_(?:[^_]+_)?' ~ stream_name_ads_details ~ '$' -%}
{%- set relations_ads_details = datacraft.get_relations_by_re(schema_pattern=target.schema, table_pattern=table_pattern_ads_details) -%}   
{%- if not relations_ads_details -%} 
    {{ exceptions.raise_compiler_error('No relations were found matching the pattern "' ~ table_pattern_ads_details ~ '". 
    Please ensure that your source data follows the expected structure.') }}
{%- endif -%}  
{%- set source_table_ads_details = '(' ~ dbt_utils.union_relations(relations_ads_details) ~ ')' -%}
{%- if not source_table_ads_details -%} 
    {{ exceptions.raise_compiler_error('No source_table were found by pattern "' ~ table_pattern_ads_details ~ '"') }}
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

WITH ads_statistics AS (
SELECT * FROM {{ source_table_ads_statistics }}
{%- if date_from and  date_to %} 
WHERE toDate(__date) between '{{date_from}}' and '{{date_to}}'
{%- endif -%}
),  

ads AS (
SELECT * FROM {{ source_table_ads_details }}
)  

SELECT
    toDate(ads_statistics.__date) AS __date,
    toLowCardinality('*') AS reportType,
    toLowCardinality(splitByChar('_', ads.__table_name)[7]) AS accountName,
    toLowCardinality(ads.__table_name) AS __table_name,
    'TG Ads' AS adSourceDirty,
    ads.title AS adCampaignName,
    ads.id AS adId,
    toFloat64(ads_statistics.spent_budget) AS adCost,
    toInt32(ads_statistics.views) AS impressions,
    toInt32(ads_statistics.joined) AS clicks,
    '' AS utmSource,
    '' AS utmMedium,
    '' AS utmCampaign,
    '' AS utmTerm,
    '' AS utmContent,
    '' AS utmHash,
    ads.__emitted_at AS __emitted_at,
    toLowCardinality('AdCostStat') AS __link 
FROM ads
JOIN ads_statistics ON ads.id = ads_statistics.ad_id
{% if limit0 %}
LIMIT 0
{%- endif -%}

{%-endif -%}
{% endmacro %}