{%- macro join_amocrm_events(
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
{#- задаём общие части имени -#}
{%- set sourcetype_name = 'amocrm' -%}
{%- set pipeline_name = 'events' -%} 
{%- set pipeline_name_registry = 'registry' -%}
{%- set template_name = 'default' -%}

{#- для каждого стрима собираем инкрементал-таблицы и создаём свой source_table_<...> -#}
{#- стрим events -#}
{%- set table_pattern_events = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name ~ '_'  ~ template_name ~  '_(?:[^_]+_)?' ~ 'events' ~ '$' -%}
{%- set relations_events = datacraft.get_relations_by_re(schema_pattern=target.schema, table_pattern=table_pattern_events) -%}   
{%- if not relations_events -%} 
    {{ exceptions.raise_compiler_error('No relations_events.
    "' ~ table_pattern_events ~ '"') }}
{%- endif -%}
{%- set source_table_events = '(' ~ dbt_utils.union_relations(relations_events) ~ ')' -%}  
{%- if not source_table_events -%} 
    {{ exceptions.raise_compiler_error('No source_table_events.
    Macro dbt_utils.union_relations fetches no data from relations_events') }}
{%- endif -%}

{#- стрим leads -#}
{%- set table_pattern_leads = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name ~ '_'  ~ template_name ~  '_(?:[^_]+_)?' ~ 'leads' ~ '$' -%}
{%- set relations_leads = datacraft.get_relations_by_re(schema_pattern=target.schema, table_pattern=table_pattern_leads) -%}   
{%- if not relations_leads -%} 
    {{ exceptions.raise_compiler_error('No relations_leads. 
    No data follows the expected pattern: "' ~ table_pattern_ad_table_pattern_leadsplans_statistics ~ '"') }}
{%- endif -%}
{%- set source_table_leads = '(' ~ dbt_utils.union_relations(relations_leads) ~ ')' -%}    
{%- if not source_table_leads -%} 
    {{ exceptions.raise_compiler_error('No source_table_leads.
    Macro dbt_utils.union_relations fetches no data from relations_leads') }}
{%- endif -%}

{#- стрим contacts -#}
{%- set table_pattern_contacts = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name ~ '_'  ~ template_name ~  '_(?:[^_]+_)?' ~ 'contacts' ~ '$' -%}
{%- set relations_contacts = datacraft.get_relations_by_re(schema_pattern=target.schema, table_pattern=table_pattern_contacts) -%}   
{%- if not relations_contacts -%} 
    {{ exceptions.raise_compiler_error('No relations_contacts. 
    No data follows the expected pattern: "' ~ table_pattern_contacts ~ '"') }}
{%- endif -%}
{%- set source_table_contacts = '(' ~ dbt_utils.union_relations(relations_contacts) ~ ')' -%}    
{%- if not source_table_contacts -%} 
    {{ exceptions.raise_compiler_error('No source_table_contacts.
    Macro dbt_utils.union_relations fetches no data from relations_contacts') }}
{%- endif -%}

{#- стрим pipelines -#}
{%- set table_pattern_pipelines = 'incremental_' ~ sourcetype_name ~ '_' ~ pipeline_name_registry ~ '_'  ~ template_name ~  '_(?:[^_]+_)?' ~ 'pipelines' ~ '$' -%}
{%- set relations_pipelines = datacraft.get_relations_by_re(schema_pattern=target.schema, table_pattern=table_pattern_pipelines) -%}   
{%- if not relations_pipelines -%} 
    {{ exceptions.raise_compiler_error('No relations_pipelines. 
    No data follows the expected pattern: "' ~ table_pattern_pipelines ~ '"') }}
{%- endif -%}
{%- set source_table_pipelines = '(' ~ dbt_utils.union_relations(relations_pipelines) ~ ')' -%}    
{%- if not source_table_pipelines -%} 
    {{ exceptions.raise_compiler_error('No source_table_pipelines.
    Macro dbt_utils.union_relations fetches no data from relations_pipelines') }}
{%- endif -%}

{#- для каждого стрима создаём его CTE -#}

{#- стрим events - Список событий – набор информации о происходящих действиях в аккаунте. -#}
with events as (
select  --__date, --дата, берётся из created_at на normalize
    toLowCardinality(splitByChar('_', __table_name)[7]) AS accountName,
    id as eventId, --ID события
    type as eventType, --Тип события 
    entity_id as entityId, --ID сущности события
    entity_type as entityType, --Сущность события
    created_by as eventCreatedBy, -- ID пользователя, создавший событие
    --ВОПРОС СОХРАНЯТЬ ЛИ ВРЕМЯ ИЛИ ДОСТАТОЧНО ДАТЫ? и нужно ли в UTC или нет? utc нужно, и осталвяем дату и время 
    toDateTime(created_at) as eventCreatedAtDateTime, -- Дата создания события, передается в Unix Timestamp
    value_after as valueAfter, -- Массив с изменениями по событию.
    value_before as valueBefore, -- Массив с изменениями по событию.
    --нужно ли тоже указывать что это аккаунт события, т.е. давать название eventAccountId? (то же в contacts) 
    account_id as systemAccountId, --ID аккаунта, в котором находится событие
    --в entityType указано к чему относится событие (task, lead, contact, talk, customer, company), а в entityId соответствующий id 
    --в отдельные столбецы вытащим id сделки и id контакта (при необходимости можно добавить будет остальные)
    case when entity_type = 'lead' THEN entity_id end as leadId,
    case when entity_type = 'contact' then entity_id end as contactId,
    -- получаем из valueAfter id актуального статуса сделки и id воронки 
    case 
        when type = 'lead_status_changed' then JSON_VALUE(value_after, '$[0].lead_status.id')
    end as statusId, -- ID статуса (соответствует statusId из стрима pipeliens)
    case 
        when type = 'lead_status_changed'  then JSON_VALUE(value_after, '$[0].lead_status.pipeline_id')
    end as pipelineId, -- ID воронки (соответствует pipelineId из стрима pipelines)
    JSONExtractString(JSONExtractRaw(JSONExtractRaw(JSONExtractRaw(_embedded, 'entity'), '_links'), 'self', 'href')) as crmSystemLinkForEvents,
    -- технические поля
    toLowCardinality('CrmEventStat') AS __link, --поле из методологии dataCraft Core (нужно для последующих шагов hash, link)
    toLowCardinality(__table_name) as __table_name,
    __emitted_at, -- дата и время выгрузки данных
    __normalized_at -- дата и время проведения нормализации (т.е. шага normalize)
from {{ source_table_events }}
-- отберём только интересующие нас события (пока отбираем только связанные со сделками и контактам)
where type in ['lead_status_changed', 'lead_deleted', 'lead_added', 'contact_added']
)

{#-  подготовительная работа:
составялем своего рода справочник, соответствия сделок и контактов
оставляем только главные конаткты
потом по leadId приджойним эту информацию к стриму leads,
а по contactId, который тут получим, приджойним данные по контактам 
-#}
, lead_contact_matching as (
select
    toLowCardinality(splitByChar('_', __table_name)[6]) AS accountName,
    account_id as systemAccountId, -- ID аккаунта, в котором находится сделка
    id as leadId, -- ID сделки
    JSONExtractString(arrayJoin(JSONExtractArrayRaw(_embedded, 'contacts')), 'id') as contactId, -- ID контакта
    JSONExtractString(arrayJoin(JSONExtractArrayRaw(_embedded, 'contacts')), 'is_main') as contactIsMain -- Является ли контакт главным для сделки
from {{ source_table_leads }}
where contactIsMain = 'true'
)

{#- стрим leads - содержит данные по сделкам -#}
, leads as (
select  --__date, --дата, берётся из created_at на NORMALIZE (нужна только дата события, дата создания сделки в поле leadCreatedAtDate)
    toLowCardinality(splitByChar('_', __table_name)[6]) AS accountName,
    id as leadId, -- ID сделки
    name as leadName, -- Название сделки
    toFloat64OrNull(price) as price, -- Бюджет сделки (в старой версии это поле назыалось sale)
    responsible_user_id as leadResponsibleUserId, -- ID пользователя, ответственного за сделку
    group_id as leadGroupId, -- ID группы, в которой состоит ответственны пользователь за сделку
    status_id as leadLastStatusId, -- ID статуса, в который добавляется сделка, по-умолчанию – первый этап главной воронки (соответствует id в _embedded[statuses] в стриме pipelines)
    pipeline_id as leadLastPipelineId, -- ID воронки, в которую добавляется сделка
    loss_reason_id as leadlossReasonId,  -- ID причины отказа
    source_id as leadSourceId,  --ID источника сделки 
    created_by as leadCreatedBy, -- ID пользователя, создающий сделку
    updated_by as leadUpdatedBy, -- ID пользователя, изменивего сделку
    --НУЖНО В UTC (сначала проверить как api отдаёт данные)
    --toDateTimeOrZero(closed_at) as leadClosedAtDate, -- Дата закрытия сделки, передается в Unix Timestamp
    toDateTime(created_at) as leadCreatedAtDate, -- Дата создания сделки, передается в Unix Timestamp
    toDateTime(updated_at) as leadUpdateddAtDate, -- Дата изменения сделки, передается в Unix Timestamp
    toDateTimeOrZero(closest_task_at) as leadClosestTaskAt, --Дата ближайшей задачи к выполнению, передается в Unix Timestamp
    is_deleted as leadIsDeleted, -- Удалена ли сделка
    -- custom_fields_values - Массив, содержащий информацию по значениям дополнительных полей, заданных для данной сделки (уточнить какие ещё данные тут могут быть нужны)
    JSONExtractArrayRaw(custom_fields_values) as customFieldsValuesArrayLeads, --также тянем целиком, на финальном шаге извлечём всё что нужно по справочнику
    --UTM_CONTENT 
    JSONExtractString(JSONExtractArrayRaw(arrayFilter(x -> JSONExtractString(x, 'field_name') = 'utm_content', 
    JSONExtractArrayRaw(custom_fields_values))[1], 'values')[1], 'value') as utmContent,
    --UTM_SOURCE 
    JSONExtractString(JSONExtractArrayRaw(arrayFilter(x -> JSONExtractString(x, 'field_name') = 'utm_source', 
    JSONExtractArrayRaw(custom_fields_values))[1], 'values')[1], 'value') as utmSource,
    --UTM_MEDIUM 
    JSONExtractString(JSONExtractArrayRaw(arrayFilter(x -> JSONExtractString(x, 'field_name') = 'utm_medium', 
    JSONExtractArrayRaw(custom_fields_values))[1], 'values')[1], 'value') as utmMedium,
    --UTM_TERM
    JSONExtractString(JSONExtractArrayRaw(arrayFilter(x -> JSONExtractString(x, 'field_name') = 'utm_term', 
    JSONExtractArrayRaw(custom_fields_values))[1], 'values')[1], 'value') as utmTerm,
    --UTM_CAMPAIGN
    JSONExtractString(JSONExtractArrayRaw(arrayFilter(x -> JSONExtractString(x, 'field_name') = 'utm_campaign', 
    JSONExtractArrayRaw(custom_fields_values))[1], 'values')[1], 'value') as utmCampaign,
    --ym_clientId
    JSONExtractString(JSONExtractArrayRaw(arrayFilter(x -> JSONExtractString(x, 'field_name') = 'yclid', 
    JSONExtractArrayRaw(custom_fields_values))[1], 'values')[1], 'value') as clientId,
    -- остальные полезные значения из custom_fields_values
    --
    toInt64OrNull(score) as leadScore, -- Скоринг сделки
    account_id as systemAccountId, -- ID аккаунта, в котором находится сделка
    toFloat64OrNull(labor_cost) as leadLaborCost, -- Тип поля "стоимость труда" показывает сколько времени было затрачено на работу со сделкой. Время исчисления в секундах
    is_price_modified_by_robot as isPriceModifiedByRobot, -- Изменен ли в последний раз бюджет сделки роботом
    -- _embedded - Данные вложенных сущностей. Нужны только данные по loss_reason (на всякий случай оставила ещё companyId) 
    -- _embedded[loss_reason] - Причина отказа сделки
    --JSONExtractString(JSONExtractArrayRaw(_embedded, 'loss_reason')[1], 'name')  as leadLossReasonName, -- Название причины отказа
    -- _embedded[contacts] - Данные контактов, привязанных к сделке (у одной сделки может быть указано более 1 контакта)
    -- для соединения данных по лидам и контактам (из стрима contacts) будет использоваться отдельная табличка LeadContactMatching (её делаю отдельно из массива _embedded[contacts])
    --
    --_embedded[companies] - Данные компании, привязанной к сделке, в данном массиве всегда 1 элемент, так как у сделки может быть только 1 компания
    JSONExtractString(JSONExtractArrayRaw(_embedded, 'companies')[1], 'id')  as leadCompanyId, -- ID компании, привязанной к сделке
    --
    JSONExtractString(JSONExtractString(JSONExtractString(arrayFilter(x -> JSONExtractBool(x, 'is_main') = true,
    JSONExtractArrayRaw(_embedded, 'contacts'))[1],'_links'),'self'),'href') as crmSystemLinkForLeads
    --технические поля, в финальную таблицу шага join идут данные из основной таблицы, к которой приджойниваем, т.е. в нашем случае из events
    --toLowCardinality('CrmEventStat') AS __link, --поле из методологии dataCraft Core (нужно для последующих шагов hash, link)
    --toLowCardinality(__table_name) as __table_name,
    --__emitted_at, -- дата и время выгрузки данных
    --__normalized_at -- дата и время проведения нормализации (т.е. шага normalize)
from {{ source_table_leads }}
)

{#- стрим contacts - данные по контактам -#}
, contacts as (
select -- __date, --дата, берётся из created_at на NORMALIZE (нужна только дата события, дата создания контакта в поле contactCreatedAtDate)
    toLowCardinality(splitByChar('_', __table_name)[6]) AS accountName,
    id as contactId, --ID контакта
    name as contactName, --Название контакта
    first_name as firstName, --Имя контакта
    last_name as lastName, --Фамилия контакта
    --custom_fields_values -- Массив, содержащий информацию по значениям дополнительных полей, заданных для данного контакта
    JSONExtractArrayRaw(custom_fields_values) as customFieldsValuesArrayContacts, --также тянем целиком, на финальном шаге извлечём всё что нужно по справочнику
    -- для телефона и email получаем вот так, так как может быть указано несколько значений: 
    --ТЕЛЕФОН
    arrayMap(x -> JSONExtractString(x, 'value'), 
        JSONExtractArrayRaw(JSONExtractString(
            arrayFilter(x -> x LIKE '%PHONE%', JSONExtractArrayRaw(custom_fields_values))[1],'values'))) AS userPhoneNumber,
    --code: Доступные значения для поля Телефон: WORK – рабочий, WORKDD – рабочий прямой, MOB – мобильный, FAX – факс, HOME – домашний, OTHER – другой.
    arrayMap(x -> JSONExtractString(x, 'enum_code'),
        JSONExtractArrayRaw(JSONExtractString(
            arrayFilter(x -> x LIKE '%PHONE%', JSONExtractArrayRaw(custom_fields_values))[1],'values'))) AS phoneCodes,
    --EMAIL
    arrayMap(x -> JSONExtractString(x, 'value'), 
        JSONExtractArrayRaw(JSONExtractString(
            arrayFilter(x -> x LIKE '%EMAIL%', JSONExtractArrayRaw(custom_fields_values))[1],'values'))) AS email,
    --code: Доступные значение для поля Email: WORK – рабочий, PRIV – личный, OTHER – другой.
    arrayMap(x -> JSONExtractString(x, 'enum_code'), 
        JSONExtractArrayRaw(JSONExtractString(
            arrayFilter(x -> x LIKE '%EMAIL%', JSONExtractArrayRaw(custom_fields_values))[1],'values'))) AS emailCodes,
    -- остальные полезные значения из custom_fields_values
    -- для пола и возраста чуть-чуть подругому, так как не может быть два значения (по логике)
    --Пол
    --JSONExtractString(JSONExtractArrayRaw(arrayFilter(x -> x LIKE '%Пол%', JSONExtractArrayRaw(custom_fields_values))[1], 'values')[1], 'value') as gender,
    -- Возраст
    --toInt8OrNull(JSONExtractString(JSONExtractArrayRaw(arrayFilter(x -> x LIKE '%Возраст%', JSONExtractArrayRaw(custom_fields_values))[1], 'values')[1], 'value')) as age,
    responsible_user_id as contactResponsibleUserId, --ID пользователя, ответственного за контакт УТОЧНИТЬ ПРО НЕЙМИНГ ПОЛЯ!!!! 
    group_id as contactGroupId, --ID группы, в которой состоит ответственны пользователь за контакт
    created_by as contactCreatedBy, -- ID пользователя, создавший контакт --ОК НАЗВАНИЕ???
    updated_by as contactUpdatedBy, --ID пользователя, изменивший контакт
    --ВОПРОС СОХРАНЯТЬ ЛИ ВРЕМЯ ИЛИ ДОСТАТОЧНО ДАТЫ? и нужно ли указывать UTC или нет? в UCT и оставляем дату и время
    toDateTime(created_at) as contactCreatedAtDate, -- Дата создания контакта, передается в Unix Timestamp 
    toDateTime(updated_at) as contactUpdatedAtDate, -- Дата изменения контакта, передается в Unix Timestamp
    is_deleted as contactIsDeleted, -- Удален ли элемент
    toDateTimeOrZero(closest_task_at) as contactClosestTaskAt, --Дата ближайшей задачи к выполнению, передается в Unix Timestamp
    account_id as systemAccountId, -- ID аккаунта, в котором находится контакт
    -- _embedded - Данные вложенных сущностей
    --_embedded[companies] - Данные компании, привязанной к контакту. В массиве всегда 1 объект (НУЖНО УТОЧНИТЬ НУЖНЫ ЛИ ЭТИ ДАННЫЕ - не нужны)
    JSONExtractString(JSONExtractArrayRaw(_embedded, 'companies')[1], 'id')  as contactCompanyId, -- ID компании, привязанной к контакту
    -- в _embedded[leads] - Данные сделок, привязанных к контакту. Одному контакту может соответствовать несколько сделок.
    -- для соединения данных (и если будет нужны графовая склейка) по лидам и контактам будет использоваться отдельная табличка LeadContactMatching (её я получаю из массива _embedded[contacts] стрима leads)
    JSONExtractString(JSONExtractArrayRaw(_embedded, 'leads')[1], 'id') as contactLeadId,
    JSONExtractString(JSONExtractString(JSONExtractString(JSONExtractArrayRaw(_embedded, 'leads')[1], '_links'),'self'), 'href') as crmSystemLinkForContacts 
    --технические поля, в финальную таблицу шага join идут данные из основной таблицы, к которой приджойниваем, т.е. в нашем случае из events
    --toLowCardinality('CrmEventStat') AS __link, --поле из методологии dataCraft Core (нужно для последующих шагов hash, link)
    --toLowCardinality(__table_name) as __table_name,
    --__emitted_at,
    --__normalized_at
from {{ source_table_contacts }}
)

{#- теперь добавляем последовательно сначала id главных контактов к стриму leads, а затем собественно информацию по контактам -#}
, leads_with_contacts as (
select *
from leads
left join lead_contact_matching using(leadId, systemAccountId, accountName) 
left join contacts using(contactId, systemAccountId, accountName) 
)

{#- стрим pipelines - содержит информацию по воронкам и шагам воронок (статусам) -#}
, pipelines as (
select  
    toLowCardinality(splitByChar('_', __table_name)[6]) AS accountName,
    id as pipelineId, --ID воронки
    name as pipelineName, -- Название воронки
    toInt8(sort) as pipelineSort, -- Сортировка воронки
    is_main as pipelineIsMain, -- Является ли воронка главной
    is_unsorted_on as pipelineIsUnsortedOn, -- Включено ли неразобранное в воронке
    is_archive as pipelineIsArchive, -- Является ли воронка архивной
    account_id as systemAccountId, -- ID аккаунта, в котором находится воронка
    -- _embedded[statuses] - Данные статусов, имеющихся в воронке.
    JSONExtractString(arrayJoin(JSONExtractArrayRaw(_embedded, 'statuses')), 'id') as statusId, --(соотв. leadStatusId в стриме leads)
    JSONExtractString(arrayJoin(JSONExtractArrayRaw(_embedded, 'statuses')), 'name') as statusName,
    toInt16(JSONExtractString(arrayJoin(JSONExtractArrayRaw(_embedded, 'statuses')), 'sort')) as statusSort,
    JSONExtractString(arrayJoin(JSONExtractArrayRaw(_embedded, 'statuses')), 'is_editable') as statusIsEditable,
    JSONExtractString(arrayJoin(JSONExtractArrayRaw(_embedded, 'statuses')), 'type') as statusType,
    JSONExtractString(JSONExtractString(JSONExtractString(arrayJoin(JSONExtractArrayRaw(_embedded, 'statuses')), '_links'), 'self'), 'href') as crmSystemLinkForStatus
    --JSONExtractString(JSONExtractString(_links, 'self'), 'href') as crmSystemLinkForPipelines
    --технические поля, в финальную таблицу шага join идут данные из основной таблицы, к которой приджойниваем, т.е. в нашем случае из events
    --toLowCardinality('CrmEventStat') AS __link, --поле из методологии dataCraft Core (нужно для последующих шагов hash, link)
    --toLowCardinality(__table_name) as __table_name,
    --__emitted_at,
    --__normalized_at
from {{ source_table_pipelines }}
)

{#- теперь делаем join записанных ранее CTE -#}
-- объединяем сначала events c leads_with_contacts
, events_leads_join as ( select
toDate(eventCreatedAtDateTime) as __date, 
l.contactId AS mainContactId,
e.contactId AS contactId,
*
from events as e
left join leads_with_contacts as l using(leadId, systemAccountId, accountName) 
left join pipelines as p using(statusId, pipelineId, systemAccountId, accountName)
-- так как потом будет union, берём только события по сделкам, чтобы не было дубля строк
WHERE eventType IN ['lead_status_changed', 'lead_deleted', 'lead_added']
)
--туперь объединям events с contacts
, events_contacts_join as ( 
select
    toDate(eventCreatedAtDateTime) as __date, 
    *
from events as e
left join contacts as l using(contactId, systemAccountId, accountName) 
-- аналогично: так как потом будет union, берём только события по контактам, чтобы не было дубля строк
where eventType IN ['contact_added']
)
-- а теперь делаем union событий по сделкам и по контактам, так в одной таблице всё сразу, если нужны будут ещё и события по контактам 
, union_events as (
---- итоговое объединение
select
    __date,
    systemAccountId,
    accountName,
    -- Поля событий
    eventId,
    eventType,
    entityId,
    entityType,
    eventCreatedBy,
    eventCreatedAtDateTime,
    valueAfter,
    valueBefore,
    crmSystemLinkForEvents,
    -- Поля статуса и воронки
    statusId,
    pipelineId,
    pipelineName,
    pipelineSort,
    pipelineIsMain,
    pipelineIsUnsortedOn,
    pipelineIsArchive,
    statusName,
    statusSort,
    statusIsEditable,
    statusType,
    crmSystemLinkForStatus,
    --crmSystemLinkForPipelines,
    -- Поля сделок
    leadId,
    leadName,
    price,
    leadResponsibleUserId,
    leadGroupId,
    leadLastStatusId,
    leadLastPipelineId,
    leadlossReasonId,
    leadSourceId,
    leadCreatedBy,
    leadUpdatedBy,
    --leadClosedAtDate,
    leadCreatedAtDate,
    leadUpdateddAtDate,
    leadClosestTaskAt,
    leadIsDeleted,
    customFieldsValuesArrayLeads,
    utmContent,
    utmSource,
    utmMedium,
    utmTerm,
    utmCampaign,
    clientId,
    leadScore,
    leadLaborCost,
    isPriceModifiedByRobot,
    leadCompanyId,
    crmSystemLinkForLeads,
    -- Поля контакта
    mainContactId,
    contactId,
    contactIsMain,
    contactName,
    firstName,
    lastName,
    userPhoneNumber,
    phoneCodes,
    email,
    emailCodes,
    --gender,
    --age,
    contactResponsibleUserId,
    contactGroupId,
    contactCreatedBy,
    contactUpdatedBy,
    contactCreatedAtDate,
    contactUpdatedAtDate,
    contactIsDeleted,
    contactClosestTaskAt,
    contactCompanyId,
    crmSystemLinkForContacts,
    -- Технические поля
    __link,
    __table_name,
    __emitted_at,
    __normalized_at
from events_leads_join
union all
select
    __date,
    systemAccountId,
    accountName,
    -- Поля событий
    eventId,
    eventType,
    entityId,
    entityType,
    eventCreatedBy,
    eventCreatedAtDateTime,
    valueAfter,
    valueBefore,
    crmSystemLinkForEvents,
    -- Поля статуса и воронки
    '' as statusId,
    '' as pipelineId,
    '' as pipelineName,
    null as pipelineSort,
    '' as pipelineIsMain,
    '' as pipelineIsUnsortedOn,
    '' as pipelineIsArchive,
    '' as statusName,
    null as statusSort,
    '' as statusIsEditable,
    '' as statusType,
    '' as crmSystemLinkForStatus,
    --'' as crmSystemLinkForPipelines,
    -- Поля сделок
    '' AS leadId,
    '' AS leadName,
    null AS price,
    '' AS leadResponsibleUserId,
    '' AS leadGroupId,
    '' AS leadLastStatusId,
    '' AS leadLastPipelineId,
    '' AS leadlossReasonId,
    '' AS leadSourceId,
    '' AS leadCreatedBy,
    '' AS leadUpdatedBy,
    --toDateTime(0) AS leadClosedAtDate,
    toDateTime(0) AS leadCreatedAtDate,
    toDateTime(0) AS leadUpdateddAtDate,
    toDateTime(0) AS leadClosestTaskAt,
    '' AS leadIsDeleted,
    [] AS customFieldsValuesArrayLeads,
    '' AS utmContent,
    '' AS utmSource,
    '' AS utmMedium,
    '' AS utmTerm,
    '' AS utmCampaign,
    '' as clientId,
    null AS leadScore,
    null AS leadLaborCost,
    '' AS isPriceModifiedByRobot,
    '' AS leadCompanyId,
    '' AS crmSystemLinkForLeads,
    -- Поля контакта
    '' AS mainContactId, 
    contactId,
    '' AS contactIsMain,
    contactName,
    firstName,
    lastName,
    userPhoneNumber,
    phoneCodes,
    email,
    emailCodes,
    --gender,
    --age,
    contactResponsibleUserId,
    contactGroupId,
    contactCreatedBy,
    contactUpdatedBy,
    contactCreatedAtDate,
    contactUpdatedAtDate,
    contactIsDeleted,
    contactClosestTaskAt,
    contactCompanyId,
    crmSystemLinkForContacts,
    -- Технические поля
    __link,
    __table_name,
    __emitted_at,
    __normalized_at
from events_contacts_join
)

select * from union_events

{% if limit0 %}
LIMIT 0
{%- endif -%}

{%- endif -%}
{% endmacro %}