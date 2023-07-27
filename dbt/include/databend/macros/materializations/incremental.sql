{% materialization incremental, adapter='databend' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {%- set unique_key = config.get('unique_key') -%}
  {% if unique_key is not none and unique_key|length == 0 %}
    {% set unique_key = none %}
  {% endif %}
  {% if unique_key is iterable and (unique_key is not string and unique_key is not mapping) %}
     {% set unique_key = unique_key|join(', ') %}
  {% endif %}
  {%- set grant_config = config.get('grants') -%}
  {%- set full_refresh_mode = (should_full_refresh() or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}

  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% set to_drop = [] %}

  {% if existing_relation is none %}
    -- No existing table, simply create a new one
    {% call statement('main') %}
        {{ get_create_table_as_sql(False, target_relation, sql) }}
    {% endcall %}

  {% elif full_refresh_mode %}
    -- Completely replacing the old table, so create a temporary table and then swap it
    {% call statement('main') %}
        {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {% endcall %}
    {% set need_swap = true %}

  {% elif unique_key is none -%}
    {% call statement('main') %}
        {{ databend__insert_into(target_relation, sql) }}
    {% endcall %}

  {% else %}
    {% set schema_changes = 'ignore' %}
    {% set incremental_strategy = config.get('incremental_strategy') %}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% if incremental_strategy != 'delete_insert' and incremental_predicates %}
      {% do exceptions.raise_compiler_error('Cannot apply incremental predicates with ' + incremental_strategy + ' strategy.') %}
    {% endif %}
    {% if incremental_strategy == 'delete_insert' %}
      {% do databend__incremental_delete_insert(existing_relation, unique_key, incremental_predicates) %}
    {% elif incremental_strategy == 'append' %}
      {% call statement('main') %}
        {{ databend__insert_into(target_relation, sql) }}
      {% endcall %}
    {% endif %}
  {% endif %}

--   {% if need_swap %}
--       {% if existing_relation.can_exchange %}
--         {% do adapter.rename_relation(intermediate_relation, backup_relation) %}
--         {% do exchange_tables_atomic(backup_relation, target_relation) %}
--       {% else %}
--         {% do adapter.rename_relation(target_relation, backup_relation) %}
--         {% do adapter.rename_relation(intermediate_relation, target_relation) %}
--       {% endif %}
--       {% do to_drop.append(backup_relation) %}
--   {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}


{% macro process_schema_changes(on_schema_change, source_relation, target_relation) %}

    {%- set schema_changes_dict = check_for_schema_changes(source_relation, target_relation) -%}
    {% if not schema_changes_dict['schema_changed'] %}
      {{ return }}
    {% endif %}

    {% if on_schema_change == 'fail' %}
      {% set fail_msg %}
          The source and target schemas on this incremental model are out of sync!
          They can be reconciled in several ways:
            - set the `on_schema_change` config to either append_new_columns or sync_all_columns, depending on your situation.
            - Re-run the incremental model with `full_refresh: True` to update the target schema.
            - update the schema manually and re-run the process.
      {% endset %}
 {% do exceptions.raise_compiler_error(fail_msg) %}
 {{ return }}
 {% endif %}

 {% do sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) %}

{% endmacro %}



{% macro databend__incremental_delete_insert(existing_relation, unique_key, incremental_predicates) %}
    {% set new_data_relation = existing_relation.incorporate(path={"identifier": model['name']
    + '__dbt_new_data_' + invocation_id.replace('-', '_')}) %}
    {{ drop_relation_if_exists(new_data_relation) }}
    {% call statement('main') %}
    {{ get_create_table_as_sql(False, new_data_relation, sql) }}
    {% endcall %}
    {% call statement('delete_existing_data') %}
delete from {{ existing_relation }} where ({{ unique_key }}) in (select {{ unique_key }}
    from {{ new_data_relation }})
  {%- if incremental_predicates %}
    {% for predicate in incremental_predicates %}
        and {{ predicate }}
    {% endfor %}
  {%- endif -%};
{% endcall %}

    {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {% call statement('insert_new_data') %}
        insert into {{ existing_relation}} select {{ dest_cols_csv}} from {{ new_data_relation }}
    {% endcall %}
    {% do adapter.drop_relation(new_data_relation) %}
{% endmacro %}
