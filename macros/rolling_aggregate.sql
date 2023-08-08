{% macro rolling_aggregate_7_periods(column_name, partition_by, order_by, num_window, function_type) %}
    {{ function_name }}( {{ column_name }} ) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
                ROWS BETWEEN {{ num_window }} PRECEDING AND CURRENT ROW
            ) AS agg_periods_{{ column_name }}
{% endmacro %}
