{%- macro attr_join_to_attr_prepare_with_qid(
  params = none,
  model_name=none,
  limit0=none,
  attributions=attribution_models(),
  events_description=events(),
  segments=event_segments()
  ) -%}

{# 
    Извлечение метаданных для определения типов моделей и их приоритетов.
#}

{# {%- set funnels = metadata['funnels'] -%}
{%- set attribution_models = metadata['attribution_models'] -%}
{%- set model_list = funnels[funnel_name].models -%}
{%- set step_name_list = funnels[funnel_name].steps -%}
{%- set steps = metadata['steps'] -%} #}

{# ****теперь не нужно**** #}
{# получаем список моделей #}
{# {% set model_list = [] %}
{% for key in attributions %}
    {% do model_list.append(key) %}
{% endfor %}
{{ log("model_list:"~model_list, true)}} #}
{# *********************** #}

{# получаем список шагов воронки #}
{% set funnel_steps = attributions[model_name]['funnel_steps'] %}
{% set step_name_list = [] %}
{% for step in funnel_steps %}
  {% do step_name_list.append(step['slug']) %}
{% endfor %}
{{log("step_name_list:"~step_name_list, true)}}

{# получаем описание шагов воронки #}
{% set steps =  events_description %}
{{log("steps:"~steps, true)}}


{# Создание словаря для хранения информации только для указанной модели (название модели берём из названия модели 
- это переменная model_name). #}
{% set model_info = {} %}

{# Проверяем, существует ли модель с указанным именем в attributions. #}
{% if model_name in attributions %}
    {% set model_data = attributions[model_name] %}
    {% set priorities = model_data.get('priorities', []) %}
    {% set model_type = model_data.get('model_type', 'unknown') %}
    {% set priority_formulas = {} %}

    {# Перебор приоритетов и сопоставление с формулами #}
    {% for priority in priorities %}
        {% if priority in segments %}
            {% set _ = priority_formulas.update({priority: segments[priority].get('formula', '')}) %}
        {% else %}
            {# Если приоритет не найден, оставляем пустую формулу #}
            {% set _ = priority_formulas.update({priority: 'Formula not found'}) %}
        {% endif %}
    {% endfor %}

    {# Обновляем model_info только для указанной модели. #}
    {% set _ = model_info.update({
        model_name: {
            'type': model_type,
            'priorities': priority_formulas
        }
    }) %}
{% else %}
    {# Логируем сообщение, если модель не найдена. #}
    {{ log("Model name '" ~ model_name ~ "' not found in attributions.", true) }}
{% endif %}

{# Лог результата #}
{{ log("Filtered model_info: " ~ model_info, true) }}

{# Лог результата #}
{{ log("model_info: " ~ model_info, true) }}

{# 
    Настройка материализации данных.
    order_by=('qid', '__period_number', '__datetime', '__priority', '__id') 
    определяет порядок сортировки данных по идентификатору группы, номеру периода, дате, приоритету и идентификатору.
#}
{{
    config(
        materialized='table',
        order_by=('qid', '__period_number', '__datetime', '__priority', '__id')
    )
}}

{# 
    Объединение результатов двух запросов: attr_prepare_with_qid и attr_create_missed_steps.
#}
select 
    y.__period_number as __period_number, 
    y.__if_missed as __if_missed, 
    y.__priority as __priority, 
    y.__step as __step,
    x.*EXCEPT(adSourceDirty),

{# 
    Вычисление ранга для каждой модели в зависимости от ее приоритета.
#}
{# {% for model_type, priorities in model_info.items() %}
    CASE
    {% for priority in priorities %}
        {%- set counter = loop.index -%}
        WHEN {{ priority }} THEN {{ counter }}
    {% endfor %}
    ELSE 0
    END as {{ '__'~ model_type ~ '_rank' }},
{% endfor %} #}

{# Вычисление ранга для каждой модели в зависимости от ее приоритета. #}
{% for model, model_data in model_info.items() %}
    {% if model_data.priorities %}
    CASE
        {% for priority, formula in model_data.priorities.items() %}
            {%- set counter = loop.index -%}
            WHEN {{ formula }} THEN {{ counter }}
        {% endfor %}
        ELSE 0
    END as {{ '__'~ model_data.type ~ '_rank' }},
    {% else %}
    0 as {{ '__'~ model_data.type ~ '_rank' }},
    {% endif %}
{% endfor %}

{# 
    Определение adSourceDirty для каждого события в зависимости от его приоритета и флага пропущенного шага.
#}
CASE
{% for step_name in step_name_list %}
    {%- set counter = loop.index -%}
    {% for step_info in steps[step_name] %}
         WHEN  __if_missed and __priority = {{ counter }} 
         THEN '{{step_info.if_missed}}'
    {% endfor %}
{% endfor %}
ELSE adSourceDirty
END as adSourceDirty

from {{ ref('attr_' ~model_name~ '_prepare_with_qid') }} AS x
join {{ ref('attr_' ~model_name~ '_create_missed_steps') }} AS y
    using (qid, __datetime, __link, __id)
{% if limit0 %}
LIMIT 0
{%- endif -%}

{% endmacro %}
