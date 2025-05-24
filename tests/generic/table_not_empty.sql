{% test table_not_empty(model) %}
select 1 as id
where not exists (select 1 from {{ model }} limit 1)
{% endtest %} 