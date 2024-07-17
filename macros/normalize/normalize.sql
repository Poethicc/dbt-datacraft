{%- macro normalize(
    fields,
    incremental_datetime_field=none, 
    incremental_datetime_formula=none,
    disable_incremental_datetime_field=none,
    defaults_dict=etlcraft.etlcraft_defaults(), 
    schema_pattern='airbyte_internal', 
    source_table=none, 
    override_target_model_name=none,
    debug_column_names=False,
    limit0=none) -%}

{#- schema_pattern=this.schema или 'airbyte_internal'-#}

{#- выполнять на втором этапе после выведения зависимостей 
первый этап - parse, здесь делается manifest, на втором этапе уже поймёт ref - возьмет его из манифеста. 
Надо завернуть в execute, иначе будет пусто -#}
{%- if execute -%}

{#- задаём части имени - либо из параметра, либо из имени файла - разбивая на части по знаку _ -#}
{%- set model_name_parts = (override_target_model_name or this.name).split('_') -%}

{#- если имя модели не соответствует шаблону - выдаём ошибку -#}
{%- if model_name_parts|length < 5 or model_name_parts[0] != 'normalize' -%}
    {{ exceptions.raise_compiler_error('Model name "' ~ this.name ~ '" does not follow the expected pattern: "normalize_{sourcetype_name}_{pipeline_name}_{template_name}_{stream_name}"') }}
{%- endif -%}

{#- задаём переменные - источник, пайплайн, шаблон, поток, и паттерн собранный из них -#}
{%- set sourcetype_name = model_name_parts[1] -%}
{%- set pipeline_name = model_name_parts[2] -%}
{%- set template_name = model_name_parts[3] -%}
{%- set last_model_name_part = model_name_parts[-1] -%}
{%- set stream_name_parts = model_name_parts[4:] -%}
{%- set stream_name = '_'.join(stream_name_parts) -%}
{#- было: set table_pattern = '_airbyte_raw_' ~ sourcetype_name ~ '_' ~ pipeline_name ~ '_' ~ template_name ~ '_[^_]+_' ~ stream_name ~ '$' -#}
{%- set table_pattern = '[^_]+' ~ '_[^_]+_' ~ 'raw__stream_' ~ sourcetype_name ~ '_' ~ template_name ~ '_[^_]+_' ~ stream_name ~ '$' -%}

{%- if pipeline_name in ('registry', 'periodstat') -%}
{%- set disable_incremental_datetime_field=true -%}
{%- endif -%}

{#- если параметр source_table при вызове макроса не задан -#}
{%- if source_table is none -%}

{#- задаём relations при помощи собственного макроса - он находится в clickhouse-adapters - найти все таблицы, которые подходят под единый шаблон, например, все mt для какого-либо проекта -#}
{#- если сырые данные лежат в той же схеме, то schema_pattern=target.schema -#}
{#- если сырые данные идут из Airbyte новой версии, то они пишутся в отдельную схему airbyte_internal -#}
{#- и поэтому для такого случая schema_pattern='airbyte_internal' из параметра -#}
{%- set relations = etlcraft.get_relations_by_re(schema_pattern=schema_pattern, 
                                                              table_pattern=table_pattern) -%}   
{#- если что-то не так - выдаём ошибку -#}                                                                  
{%- if not relations -%}
    {{ exceptions.raise_compiler_error('No relations were found matching the pattern "' ~ table_pattern ~ '". Please ensure that your source data follows the expected structure.') }}
{%- endif -%}

{#- собираем одинаковые таблицы, которые будут проходить по этому макросу  - здесь union all найденных таблиц -#}
{%- set source_table = '(' ~ etlcraft.custom_union_relations_source(relations) ~ ')' -%} 
{%- endif -%} {# конец для if source_table  #}

{#- ищем формулу для поля с датой -#}
{%- if incremental_datetime_formula is none and disable_incremental_datetime_field is none -%}
    {#- задаём переменную incremental_datetime_formula -#}
    {%- set incremental_datetime_formula = etlcraft.get_from_default_dict(defaults_dict, ['sourcetypes', sourcetype_name, 'streams', stream_name, 'incremental_datetime_formula'], default_return=none) -%}
    {%- if not incremental_datetime_formula -%}
    {%- set incremental_datetime_formula = etlcraft.get_from_default_dict(defaults_dict, ['sourcetypes', sourcetype_name, 'incremental_datetime_formula'], default_return=none) -%}
    {%- endif -%}
{%- endif -%}
{%- if incremental_datetime_formula -%}
{%- set incremental_datetime_field = '__date' -%}
{%- endif -%}
{#- если при вызове макроса параметры не заданы -#}
{%- if incremental_datetime_field is none and disable_incremental_datetime_field is none -%}
    {#- задаём переменную incremental_datetime_field -#}
    {%- set incremental_datetime_field = etlcraft.find_incremental_datetime_field(fields, override_target_model_name or this.name, defaults_dict=defaults_dict) or '' -%}
{%- endif -%}

{%- set column_list = [] -%}

{#- для каждого столбца в полученном наборе -#}
{%- for key in fields -%}        
    {#- если оно не является инкрементальным полем с датой 
    {%- if  key != incremental_datetime_field -%} -#}
        {#- устанавливаем псевдоним - делаем транслитерацию на англ -#}
        {%- set alias = etlcraft.normalize_name(key) -%}
        {%- set column_value = etlcraft.json_extract_string('_airbyte_data', key) if not debug_column_names else "'" ~ alias ~ "'" -%}
        {#- в итоговый список добавялем обработанное значение столбца -#}
        {%- do column_list.append(column_value ~ " AS " ~ alias) -%}
    {#-{%- endif -%} -#}
{%- endfor -%}

{#- сортируем итоговый список полей - по алфавиту -#}
{%- set column_list = column_list | sort -%}
{#- преобразование для инкрементального поля с датой -#}
{%- if incremental_datetime_field -%}
    {%- if incremental_datetime_formula -%}
    {% set column_value = incremental_datetime_formula|replace('__date', etlcraft.json_extract_string('_airbyte_data', incremental_datetime_field)) %}  
    {%- else -%}      
    {%- set column_value = etlcraft.json_extract_string('_airbyte_data', incremental_datetime_field) if not debug_column_names else "'__date'" -%}    
    {%- endif -%}    
    {%- set column_list = [column_value ~ " AS __date"] + column_list -%}
{%- endif -%}
{#- условие для пустого итогового списка -#}
{%- if column_list | length == 0 -%}
    {{ exceptions.raise_compiler_error('Normalize returned empty column list') }}
{%- endif -%}

{#- в разных версиях Airbyte поле называется: _airbyte_extracted_at или _airbyte_emitted_at -#}
{#- поэтому делаем проверку и условие - чтобы макрос работал с обоими вариантами -#}
{% set is_airbyte_extracted_at %}
SELECT _airbyte_extracted_at
FROM {{ source_table }}
LIMIT 1
{% endset %}
{% set result_extracted_at = run_query(is_airbyte_extracted_at) %}

{#- это самое важное - что мы видим в модели - SELECT итогового списка полей + технические поля -#}
SELECT
        {{ column_list | join(', \n        ') }},
        toLowCardinality(_dbt_source_relation) AS __table_name,  
    {%- if result_extracted_at %}
        toDateTime32(substring(toString(_airbyte_extracted_at), 1, 19)) AS __emitted_at, 
    {%- else -%}
        toDateTime32(substring(toString(_airbyte_emitted_at), 1, 19)) AS __emitted_at, 
    {%- endif %}
        NOW() AS __normalized_at
FROM {{ source_table }}
{%- if limit0 -%}
LIMIT 0
{%- endif -%}


{%- endif -%} {# конец для if execute #}
{%- endmacro -%}
