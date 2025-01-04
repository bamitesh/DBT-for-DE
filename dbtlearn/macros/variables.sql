{% macro learn_variables() %}
    {% set your_name_jinja = "Amitesh" %}
    {{ log (
        "Hello " ~ your_name_jinja,
        info = True
    ) }}
    {{ log(
        "Hello dbt user " ~ var(
            "user_name",
            "NO Username is Set!!"
        ) ~ "!",
        info = True
    ) }}
{% endmacro %}
