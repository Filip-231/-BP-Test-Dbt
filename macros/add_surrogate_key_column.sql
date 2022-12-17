{% macro add_surrogate_key_column(field_list) -%}
  {{ dbt_utils.surrogate_key(field_list) }} AS `SurrogateKey`
{%- endmacro %}
