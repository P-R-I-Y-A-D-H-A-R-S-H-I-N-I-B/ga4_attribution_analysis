{% macro get_coalesce_user() %}
COALESCE(user_id, user_pseudo_id)
{% endmacro %}
