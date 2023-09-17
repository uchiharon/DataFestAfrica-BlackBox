{% macro remove_nulls_from_table(table_name) %}
    -- Generate SQL to remove nulls for each column in the specified table
    {% set remove_null_statements = [] %}
    {% for column in run_query("SELECT column_name FROM information_schema.columns WHERE table_name = '" ~ table_name ~ "'") %}
        {% set column_name = column.column_name %}
        {% set remove_null_sql = "UPDATE " ~ table_name ~ " SET " ~ column_name ~ " = '' WHERE " ~ column_name ~ " IS NULL;" %}
        {% do remove_null_statements.append(remove_null_sql) %}
    {% endfor %}
    
    -- Execute the generated SQL statements for the specified table
    {% for statement in remove_null_statements %}
        {{ statement }}
    {% endfor %}
{% endmacro %}
