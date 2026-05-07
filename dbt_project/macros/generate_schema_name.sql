-- Override dbt's default schema naming behaviour.
--
-- By default dbt generates schema names as `<target_schema>_<custom_schema>`
-- (e.g. "main_marts", "main_staging"). This macro makes dbt use the custom
-- schema name directly (e.g. "marts", "staging", "raw"), which keeps the
-- DuckDB schema names consistent with what the Dagster extraction assets write
-- and what the export assets read.
--
-- Models/seeds without a custom schema fall back to the target schema ("main").

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
