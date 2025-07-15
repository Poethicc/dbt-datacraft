{% macro find_incremental_datetime_field(column_list, relation, defaults_dict=datacraft.datacraft_defaults(), do_not_throw=False) %}        
    {%- set model_name_parts = relation.split('_') -%}
    {%- if model_name_parts|length <  4 or model_name_parts[0] != 'normalize' -%}
        {{ exceptions.raise_compiler_error('Relation identifier "' ~ relation.identifier ~ '" does not follow the expected pattern: "{normalize}_{sourcetype_name}_{pipeline_name}_{template_name}_{stream_name}"') }}
    {%- endif -%}
    {%- set sourcetype_name = model_name_parts[1] -%}
    {%- set pipeline_name = model_name_parts[2] -%}
    {%- set template_name = model_name_parts[3] -%}
    {%- set stream_name_parts = model_name_parts[4:] -%}
    {%- set stream_name = '_'.join(stream_name_parts) -%}
        
    {%- set datetime_field = datacraft.get_from_default_dict(defaults_dict, ['sourcetypes', sourcetype_name, 'streams', stream_name, 'incremental_datetime_field']) -%}
    {%- if datetime_field == {} -%}
        {%- set datetime_field = datacraft.get_from_default_dict(defaults_dict, ['sourcetypes', sourcetype_name, 'incremental_datetime_field']) -%}
    {%- endif -%}
    
    {%- if datetime_field == {} -%}
        {%- set datetime_columns = [] -%}        
        {%- for column in column_list -%}            
            {%- if  column.lower().endswith('date') 
              or column.lower().endswith('datetime')
              or column.lower().endswith('day')
              or column.lower().endswith('period_start')
              or column.lower().endswith('timestamp')
              or column.lower().endswith('_time') -%}
                {%- do datetime_columns.append(column) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if datetime_columns|length == 0 and not do_not_throw -%}
            {{ exceptions.raise_compiler_error('No columns with names ending in "date", "datetime", "timestamp", "_time" were found in the relation "' ~ relation ~ '". Please ensure that your source data contains such a column for incremental processing.') }}
        {%- elif datetime_columns|length > 1 and not do_not_throw -%}
            {{ exceptions.raise_compiler_error('Multiple columns with names  ending in "date", "datetime", "timestamp", "_time" were found in the relation "' ~ relation ~ '". Please ensure that only one such column is present for incremental processing, or specify the correct one in the defaults dictionary.') }}
        {%- else -%}
            {%- set datetime_field = datetime_columns[0] -%}
        {%- endif -%}
    {%- endif -%}
    {{ return(datetime_field) }}
{%- endmacro -%}



