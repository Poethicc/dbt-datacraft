{%- macro join_yd_datestat(
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
{%- set sourcetype_name = 'yd' -%}
{%- set pipeline_name = 'datestat' -%}
{%- set template_name = 'default' -%}

{%- set table_pattern = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name ~ '_' ~ template_name ~  '_[^_]+'  -%}
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

WITH cmps AS (
SELECT * FROM {{ source_table }}
{%- if date_from and  date_to %} 
WHERE toDate(__date) BETWEEN '{{date_from}}' AND '{{date_to}}'
{%- endif -%}
)

SELECT  
    toDate(__date) AS __date,
    toLowCardinality('*') AS reportType, 
    toLowCardinality(splitByChar('_', __table_name)[7]) AS accountName,
    toLowCardinality(__table_name) AS __table_name,
    'Yandex Direct Ads' AS adSourceDirty,
    --'' AS productName,
    CampaignName AS adCampaignName,
    --CampaignType AS adGroupName,
    CampaignId AS adId,
    (toFloat64(Cost)/1000000) AS adCost,
    toInt32(Impressions) AS impressions,
    toInt32(Clicks) AS clicks,
    --'' AS adPhraseId,
    '' AS utmSource,
    '' AS utmMedium,
    '' AS utmCampaign,
    '' AS utmTerm,
    '' AS utmContent,
    arrayElement(splitByChar('~', CampaignName), 2) AS utmHash,
    --'' AS adTitle1,
    --'' AS adTitle2,
    --'' AS adText,
    --'' AS adPhraseName,  
    __emitted_at,
    toLowCardinality('AdCostStat') AS __link 
FROM cmps
{% if limit0 %}
LIMIT 0
{%- endif -%}

{%-endif -%}
{% endmacro %}