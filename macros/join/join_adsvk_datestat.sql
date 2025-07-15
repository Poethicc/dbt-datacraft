{%- macro join_adsvk_datestat(
    sourcetype_name,
    pipeline_name,
    relations_dict,
    date_from,
    date_to,
    params,
    limit0=none
    ) -%}


{#- внизу в запросе хардкод accountName -#}


{{ config(
    materialized='incremental',
    order_by=('__date', '__table_name'),
    incremental_strategy='delete+insert',
    unique_key=['__date', '__table_name'],
    on_schema_change='fail'
) }}

{%- if execute -%}
{%- set sourcetype_name = 'adsvk' -%}
{%- set pipeline_name_datestat = 'datestat' -%} 
{%- set pipeline_name_registry = 'registry' -%}
{%- set template_name = 'default' -%}

{%- set stream_name_ad_plans_statistics = 'ad_plans_statistics' -%}
{%- set table_pattern_ad_plans_statistics = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name_datestat ~ '_' ~ template_name ~  '_(?:[^_]+_)?' ~ stream_name_ad_plans_statistics ~ '$' -%}
{%- set relations_ad_plans_statistics = datacraft.get_relations_by_re(schema_pattern=target.schema, table_pattern=table_pattern_ad_plans_statistics) -%}   
{%- if not relations_ad_plans_statistics -%} 
    {{ exceptions.raise_compiler_error('No relations were found matching the pattern "' ~ table_pattern_ad_plans_statistics ~ '". 
    Please ensure that your source data follows the expected structure.') }}
{%- endif -%} 
{%- set source_table_ad_plans_statistics = '(' ~ dbt_utils.union_relations(relations_ad_plans_statistics) ~ ')' -%} 
{%- if not source_table_ad_plans_statistics -%} 
    {{ exceptions.raise_compiler_error('No source_table were found by pattern "' ~ table_pattern_ad_plans_statistics ~ '"') }}
{%- endif -%} 

{%- set stream_name_ad_plans = 'ad_plans' -%}
{%- set table_pattern_ad_plans = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name_registry ~ '_' ~ template_name ~  '_[^_]+_' ~ stream_name_ad_plans ~ '$' -%}
{%- set relations_ad_plans = datacraft.get_relations_by_re(schema_pattern=target.schema, table_pattern=table_pattern_ad_plans) -%}   
{%- if not relations_ad_plans -%} 
    {{ exceptions.raise_compiler_error('No relations were found matching the pattern "' ~ table_pattern_ad_plans ~ '". 
    Please ensure that your source data follows the expected structure.') }}
{%- endif -%}  
{%- set source_table_ad_plans = '(' ~ dbt_utils.union_relations(relations_ad_plans) ~ ')' -%}
{%- if not source_table_ad_plans -%} 
    {{ exceptions.raise_compiler_error('No source_table were found by pattern "' ~ table_pattern_ad_plans ~ '"') }}
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

WITH ad_plans_statistics AS (
SELECT * FROM {{ source_table_ad_plans_statistics }}
{%- if date_from and  date_to %} 
WHERE toDate(__date) between '{{date_from}}' and '{{date_to}}'
{%- endif -%}
),  

ad_plans AS (
SELECT * FROM {{ source_table_ad_plans }}
)  

SELECT
    toDate(ad_plans_statistics.__date) AS __date,
    toLowCardinality('*') AS reportType,
    toLowCardinality('18823536') AS accountName,
    toLowCardinality(ad_plans.__table_name) AS __table_name,
    'VK Ads' AS adSourceDirty,
    ad_plans.name AS adCampaignName,
    ad_plans.id AS adId,
    toFloat64(JSONExtractString(ad_plans_statistics.base, 'spent')) AS adCost,
    toInt32(JSONExtractString(ad_plans_statistics.base, 'shows')) AS impressions,
    toInt32(JSONExtractString(ad_plans_statistics.base, 'clicks')) AS clicks,
    '' AS utmSource,
    '' AS utmMedium,
    '' AS utmCampaign,
    '' AS utmTerm,
    '' AS utmContent,
    '' AS utmHash,
    ad_plans.__emitted_at AS __emitted_at,
    toLowCardinality('AdCostStat') AS __link 
FROM ad_plans
JOIN ad_plans_statistics ON ad_plans.id = ad_plans_statistics.ad_plan_id
{% if limit0 %}
LIMIT 0
{%- endif -%}

{%-endif -%}
{% endmacro %}