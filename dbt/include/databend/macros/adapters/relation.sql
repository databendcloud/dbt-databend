{% macro databend__get_or_create_relation(database, schema, identifier, type) %}
  {%- set target_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
  {% if target_relation %}
    {% do return([true, target_relation]) %}
  {% endif %}

  {%- set new_relation = api.Relation.create(
      database=database,
      schema=schema,
      identifier=identifier,
      type=type
  ) -%}
  {% do return([false, new_relation]) %}
{% endmacro %}

{% macro databend__get_database(database) %}
    {% call statement('get_database', fetch_result=True) %}
select name
from system.databases
where name = '{{ database }}'
    {% endcall %}
   {% do return(load_result('get_database').table) %}
{% endmacro %}