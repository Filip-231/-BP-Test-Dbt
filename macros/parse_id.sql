{% macro parse_id(column_name) -%}
  CAST(REGEXP_EXTRACT({{ column_name }}, r"{{ var('id_regex') }}") AS INTEGER)
{%- endmacro %}
