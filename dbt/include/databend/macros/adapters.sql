/* For examples of how to fill out the macros please refer to the postgres adapter and docs
postgres adapter macros: https://github.com/dbt-labs/dbt-core/blob/main/plugins/postgres/dbt/include/postgres/macros/adapters.sql
dbt docs: https://docs.getdbt.com/docs/contributing/building-a-new-adapter
*/

{% macro databend__create_schema(relation) -%}
'''Creates a new schema in the  target database, if schema already exists, method is a no-op. '''
  {%- call statement('create_schema') -%}
    create database if not exists {{ relation.without_identifier().include(database=False) }}
  {% endcall %}
{% endmacro %}

{% macro databend__drop_relation(relation) -%}
'''Deletes relatonship identifer between tables.'''
/*
  1. If database exists
  2. Create a new schema if passed schema does not exist already
*/
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro databend__drop_schema(relation) -%}
'''drops a schema in a target database.'''
/*
  1. If database exists
  2. search all calls of schema, and change include value to False, cascade it to backtrack
*/
  {%- call statement('drop_schema') -%}
    drop database if exists {{ relation.without_identifier().include(database=False) }}
  {%- endcall -%}
{% endmacro %}

{% macro databend__get_columns_in_relation(relation) -%}
'''Returns a list of Columns in a table.'''
/*
  1. select as many values from column as needed
  2. search relations to columns
  3. where table name is equal to the relation identifier
  4. if a relation schema exists and table schema is equal to the relation schema
  5. order in whatever way you want to call.
  6. create a table by loading result from call
  7. return new table
*/
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    select
      name,
      type,
      0 as position
    from system.columns
    where
      table = '{{ relation.identifier }}'
    {% if relation.schema %}
      and database = '{{ relation.schema }}'
    {% endif %}
  {% endcall %}
  {% do return(load_result('get_columns_in_relation').table) %}
{% endmacro %}

--  Example of 2 of 3 required macros that do not come with a default implementation

{% macro databend__list_relations_without_caching(schema_relation) -%}
'''creates a table of relations withough using local caching.'''
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      null as db,
      name as name,
      database as schema,
      if(engine = 'VIEW', 'view', 'table') as type
    from system.tables
    where database = '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro databend__list_schemas(database) -%}
'''Returns a table of unique schemas.'''
/*
  1. search schemea by specific name
  2. create a table with names
*/
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select name from system.databases
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro databend__rename_relation(from_relation, to_relation) -%}
'''Renames a relation in the database.'''
/*
  1. Search for a specific relation name
  2. alter table by targeting specific name and passing in new name
*/
  {% call statement('drop_relation') %}
    drop table if exists {{ to_relation }}
  {% endcall %}
  {% call statement('rename_relation') %}
    rename table {{ from_relation }} to {{ to_relation }}
  {% endcall %}
{% endmacro %}

{% macro databend__truncate_relation(relation) -%}
'''Removes all rows from a targeted set of tables.'''
/*
  1. grab all tables tied to the relation
  2. remove rows from relations
*/
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }}
  {%- endcall %}
{% endmacro %}


{% macro databend__make_temp_relation(base_relation, suffix) %}
  {% set tmp_identifier = base_relation.identifier ~ suffix %}
  {% set tmp_relation = base_relation.incorporate(
                              path={"identifier": tmp_identifier, "schema": None}) -%}
  {% do return(tmp_relation) %}
{% endmacro %}

{% macro databend__generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(None) %}
{%- endmacro %}

{% macro databend__current_timestamp() -%}
  NOW()
{%- endmacro %}

{% macro databend__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  {% if temporary -%}
    create transient table {{ relation.name }} if not exist
  {%- else %}
    create table {{ relation.include(database=False) }}
    {{ cluster_by_clause(label="cluster by") }}
  {%- endif %}
  as {{ sql }}
{%- endmacro %}

{% macro databend__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create view {{ relation.include(database=False) }}
  as {{ sql }}
{%- endmacro %}

{% macro databend__get_columns_in_query(select_sql) %}
  {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
    select * from (
        {{ select_sql }}
    ) as __dbt_sbq
    limit 0
  {% endcall %}

  {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro cluster_by_clause(label) %}
  {%- set cols = config.get('cluster_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }}
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
  {%- endif %}
{%- endmacro -%}