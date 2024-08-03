-- macros/alter_table.sql
{% macro alter_table_set_not_null (schema, table, column) %}
  ALTER TABLE {{ schema }}.{{ table }} MODIFY COLUMN {{ column }} SET NOT NULL
  /*ALTER TABLE {{ dbtsf_schema }}.{{ PLAYER_CLEAN_TBL }} MODIFY COLUMN {{ match_type_number }} SET NOT NULL 
  dbt run-operation alter_table_set_not_null --args '{"schema": "dbtsf_schema", "table": "PLAYER_CLEAN_TBL", "column": "match_type_number"}'
  */
{% endmacro %}
