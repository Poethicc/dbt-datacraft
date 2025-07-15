{%- macro join_vkads_datestat(
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
{%- set sourcetype_name = 'vkads' -%}
{%- set pipeline_name_datestat = 'datestat' -%} 
{%- set pipeline_name_registry = 'registry' -%}
{%- set template_name = 'default' -%}

{%- set stream_name_campaign_statistics = 'campaign_statistics' -%}
{%- set table_pattern_campaign_statistics = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name_datestat ~ '_' ~ template_name ~  '_(?:[^_]+_)?' ~ stream_name_campaign_statistics ~ '$' -%}
{%- set relations_campaign_statistics = datacraft.get_relations_by_re(schema_pattern=target.schema, table_pattern=table_pattern_campaign_statistics) -%}   
{%- if not relations_campaign_statistics -%} 
    {{ exceptions.raise_compiler_error('No relations were found matching the pattern "' ~ table_pattern_campaign_statistics ~ '". 
    Please ensure that your source data follows the expected structure.') }}
{%- endif -%} 
{%- set source_table_campaign_statistics = '(' ~ dbt_utils.union_relations(relations_campaign_statistics) ~ ')' -%} 
{%- if not source_table_campaign_statistics -%} 
    {{ exceptions.raise_compiler_error('No source_table were found by pattern "' ~ table_pattern_campaign_statistics ~ '"') }}
{%- endif -%} 

{%- set stream_name_campaign = 'campaigns' -%}
{%- set table_pattern_campaign = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name_registry ~ '_' ~ template_name ~  '_[^_]+_' ~ stream_name_campaign ~ '$' -%}
{%- set relations_campaign = datacraft.get_relations_by_re(schema_pattern=target.schema, table_pattern=table_pattern_campaign) -%}   
{%- if not relations_campaign -%} 
    {{ exceptions.raise_compiler_error('No relations were found matching the pattern "' ~ table_pattern_campaign ~ '". 
    Please ensure that your source data follows the expected structure.') }}
{%- endif -%}  
{%- set source_table_campaign = '(' ~ dbt_utils.union_relations(relations_campaign) ~ ')' -%}
{%- if not source_table_campaign -%} 
    {{ exceptions.raise_compiler_error('No source_table were found by pattern "' ~ table_pattern_campaign ~ '"') }}
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

WITH campaign_statistics AS (
SELECT * FROM {{ source_table_campaign_statistics }}
{%- if date_from and  date_to %} 
WHERE toDate(__date) between '{{date_from}}' and '{{date_to}}'
{%- endif -%}
),  

campaign AS (
SELECT * FROM {{ source_table_campaign }}
)  

SELECT
    toDate(campaign_statistics.__date) AS __date,
    toLowCardinality('*') AS reportType,
    toLowCardinality(splitByChar('_', campaign.__table_name)[7]) AS accountName,
    toLowCardinality(campaign.__table_name) AS __table_name,
    'VK Ads (old)' AS adSourceDirty,
    campaign.name AS adCampaignName,
    campaign.id AS adId,
    toFloat64OrZero(campaign_statistics.spent) AS adCost,
    toInt32OrZero(campaign_statistics.impressions) AS impressions,
    toInt32OrZero(campaign_statistics.clicks) AS clicks,
    '' AS utmSource,
    '' AS utmMedium,
    '' AS utmCampaign,
    '' AS utmTerm,
    '' AS utmContent,
    '' AS utmHash,
    campaign.__emitted_at AS __emitted_at,
    toLowCardinality('AdCostStat') AS __link 
FROM campaign
JOIN campaign_statistics ON campaign.id = campaign_statistics.campaign_id
{% if limit0 %}
LIMIT 0
{%- endif -%}

{%-endif -%}
{% endmacro %}