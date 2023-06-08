
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}
{%- endmacro %}

{#
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if target.name[-3:] == 'sandbox' -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}

    {%- elif target.schema[:9] == 'dbt_cloud' -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}

    {%- elif custom_schema_name is none -%}
        {{ default_schema }}

    {%- else -%}
        {{ custom_schema_name | trim }}

    {%- endif -%}
{%- endmacro %}
#}

{% macro set_query_tag() -%}
  {# --TODO: extend this for tests once new test PR is merged #}

  {# -- These are built in dbt Cloud environment variables you can leverage to better understand your runs usage data #}
  {% set dbt_project_id = env_var('DBT_CLOUD_PROJECT_ID', 'not set') %}
  {% set dbt_job_id = env_var('DBT_CLOUD_JOB_ID', 'not set') %}
  {% set dbt_run_id = env_var('DBT_CLOUD_RUN_ID', 'not set') %}
  {% set dbt_run_reason_category = env_var('DBT_CLOUD_RUN_REASON_CATEGORY', 'not set') %}
  {% set dbt_run_reason = env_var('DBT_CLOUD_RUN_REASON', 'development_and_testing') %}

  {# -- These are built in to dbt Core #}
  
  {% set dbt_version = dbt_version %}
  {% set dbt_project_name = project_name %}
  {% set dbt_invocation_id = invocation_id %}
  {% set dbt_user_name = target.user %}
  {% set dbt_profile_name = target.profile_name %}  
  {% set dbt_db_user_role = target.role %}
  {% set dbt_db_account_name = target.account %}  
  {% set dbt_db_warehouse = target.warehouse %}  
  {% set dbt_database = target.database %}  
  {% set dbt_schema = target.schema %}  

  {% set dbt_model_unique_id = model.unique_id %} 
  {% set dbt_model_name = model.name %}
  {% set dbt_model_alias = model.alias %}
  {% set dbt_model_original_file_path = model.original_file_path %}
  {% set dbt_materialization_type = model.config.materialized %}
  {% set dbt_incremental_full_refresh = 'false' %}
  {% set dbt_environment_name = target.name %}

  {% if dbt_materialization_type == 'incremental' and should_full_refresh() %}
    {% set dbt_incremental_full_refresh = 'true' %}
  {% endif %}

  {% if dbt_model_name %}
 {#    
    {% set new_query_tag = '{"dbt_environment_name": "%s", "dbt_job_id": "%s", "dbt_run_id": "%s", "dbt_run_reason": "%s", "dbt_project_name": "%s", "dbt_user_name": "%s", "dbt_model_name": "%s", "dbt_materialization_type": "%s", "dbt_incremental_full_refresh": "%s"}'
#}
    {% set new_query_tag = '{"dbt_version": "%s", "dbt_environment_name": "%s", "dbt_invocation_id": "%s", "dbt_job_id": "%s", "dbt_run_id": "%s", "dbt_run_reason_category": "%s", "dbt_run_reason": "%s", "dbt_project_id": "%s", "dbt_project_name": "%s", "dbt_user_name": "%s", "dbt_profile_name": "%s", "dbt_db_user_role": "%s", "dbt_db_account_name": "%s", "dbt_database": "%s", "dbt_schema": "%s", "dbt_db_warehouse": "%s", "dbt_model_name": "%s", "dbt_model_unique_id": "%s", "dbt_model_alias": "%s", "dbt_model_original_file_path": "%s", "dbt_materialization_type": "%s", "dbt_incremental_full_refresh": "%s"}'
      |format(dbt_version,
              dbt_environment_name,
              dbt_invocation_id,
              dbt_job_id,
              dbt_run_id,
              dbt_run_reason_category,
              dbt_run_reason,
              dbt_project_id,
              dbt_project_name,
              dbt_user_name,
              dbt_profile_name,
              dbt_db_user_role,
              dbt_db_account_name,
              dbt_database,
              dbt_schema,
              dbt_db_warehouse,
              dbt_model_name,
              dbt_model_unique_id,
              dbt_model_alias,
              dbt_model_original_file_path,
              dbt_materialization_type,
              dbt_incremental_full_refresh) %}
    {% set original_query_tag = get_current_query_tag() %}
    {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
    {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
    {{ return(original_query_tag)}}
  
  {% endif %}
  
  {{ return(none)}}

{% endmacro %}

{#
{% macro set_query_tag() -%}
  {% set new_query_tag = this %} 
  {% if new_query_tag %}
    {% set original_query_tag = get_current_query_tag() %}
    {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
    {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
    {{ return(original_query_tag)}}
  {% endif %}
  {{ return(none)}}
{% endmacro %}
#}
