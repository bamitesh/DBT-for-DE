{% macro learn_logging() %}
    {{ log(
        "Call dbt function!",
        info = True
    ) }}
{% endmacro %}
