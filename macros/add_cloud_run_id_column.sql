{% macro add_cloud_run_id_column() -%}
  {{ env_var("DBT_CLOUD_RUN_ID", 0) }} AS `CloudRunId`
{%- endmacro %}
