# ============================= VARIABLE DEFINITIONS =============================
correlated_column, other_column = "a", "b"
subquery_alias = "subquery_alias"
subquery_set_operation_alias = "subquery_set_operation_alias"
subquery_column_alias = "subquery_column_alias"

inner_table = "inner_table"
outer_table = "outer_table"
# No matching rows for correlated column with other tables.
no_match_table = "no_match_table"
# Table with only one row, (NULL, NULL).
null_table = "null_table"
# Used for join and set operations
join_table = "join_table"

table_creation_sql = f"""CREATE TEMPORARY VIEW {inner_table} (a, b) AS VALUES
    (1, 10),
    (2, 20),
    (3, 30),
    (4, 40),
    (5, 50),
    (8, 80),
    (9, 90);
CREATE TEMPORARY VIEW {outer_table} (a, b) AS VALUES
    (1, 100),
    (2, 200),
    (3, 300),
    (4, 400),
    (6, 600),
    (7, 700),
    (10, 1000);
CREATE TEMPORARY VIEW {no_match_table} (a, b) AS VALUES
    (9999, 9999);
CREATE TEMPORARY VIEW {join_table} (a, b) AS VALUES
    (1, 10),
    (3, 30),
    (4, 400),
    (6, 600),
    (8, 80);
CREATE TEMPORARY VIEW null_table (a, b) AS SELECT CAST(null AS int), CAST(null as int);
"""

# Query clauses.
SELECT, FROM, WHERE = "SELECT", "FROM", "WHERE"

# Subquery type.
IN, NOT_IN, EXISTS, NOT_EXISTS, SCALAR = "IN", "NOT IN", "EXISTS", "NOT EXISTS", "="

# Aggregate function types.
SUM, COUNT = "SUM", "COUNT"

# Join types.
INNER, LEFT_OUTER, RIGHT_OUTER = "INNER JOIN", "LEFT OUTER JOIN", "RIGHT OUTER JOIN"

# Set operations.
INTERSECT, UNION, EXCEPT = "INTERSECT", "UNION", "EXCEPT"

# ============================= SUBQUERY VARIATIONS =============================

table_combinations = [
    (inner_table, outer_table),
    (inner_table, null_table),
    (null_table, outer_table),
    (no_match_table, outer_table),
    (inner_table, no_match_table),
]

# Subquery types.
subquery_types = [IN, NOT_IN, EXISTS, NOT_EXISTS, SCALAR]

# Subquery properties - correlated or not.
correlated = [True, False]

# Distinct subquery projection.
project_distinct = [True, False]

# Query clause in which subquery is in.
subquery_clauses = [SELECT, FROM, WHERE]

# Subquery operators
AGGREGATE, LIMIT, WINDOW, ORDER_BY, JOIN, SET_OP = (
    "AGGREGATE",
    "LIMIT",
    "WINDOW",
    "ORDER BY",
    "JOIN",
    "SET_OP",
)

# Tuples of (aggregateFunction, groupBy: bool)
aggregation_functions = [(SUM, True), (SUM, False), (COUNT, True), (SUM, False)]

# Values for limit.
limit_values = [1, 10]

# TODO: add window functions
subquery_operators = {
    AGGREGATE: aggregation_functions,
    LIMIT: limit_values,
    ORDER_BY: [None],
    JOIN: [INNER, LEFT_OUTER, RIGHT_OUTER],
    SET_OP: [INTERSECT, UNION, EXCEPT],
}

# ============================= FUNCTION FOR SUBQUERY COMBINATIONS =============================


def generate_subquery(
    innertable,
    outertable,
    subquery_type,
    is_correlated,
    distinct,
    operator,
    operator_specification,
):
    """
    Generate a subquery with specified parameters.

    Parameters:
    - innertable: Inner table for the subquery.
    - outertable: Outer table for correlation conditions.
    - subquery_type: Type of subquery (SCALAR, EXISTS, etc.).
    - is_correlated: True if the subquery is correlated, False otherwise.
    - distinct: True if DISTINCT is applied, False otherwise.
    - operator: Subquery operator (JOIN, SET_OP, AGGREGATE, etc.).
    - operator_specification: Specification for the subquery operator.
      - For AGGREGATE: Tuple (aggregate_function, group_by).
      - For LIMIT: Limit value.
      - For JOIN: Join operator.
    - Returns a tuple containing the generated SQL string and the projection column alias.
    """
    aggregate_function, group_by = (
        operator_specification if operator == AGGREGATE else (None, None)
    )
    limit_value = operator_specification if operator == LIMIT else (None, None)

    # FROM CLAUSE OF SUBQUERY - apply JOINS and SET OPERATIONS
    table_alias = subquery_set_operation_alias if operator == SET_OP else innertable
    from_clause = "FROM "
    if operator == SET_OP:
        set_operator = operator_specification
        from_clause += (
            f"(SELECT a, b FROM {innertable} {set_operator} "
            f"SELECT a, b FROM {join_table}) AS {table_alias}"
        )
    elif operator == JOIN:
        join_operator = operator_specification
        from_clause += (
            f"{innertable} {join_operator} {join_table}"
            f" ON {innertable}.{other_column} = {join_table}.{other_column}"
        )
    else:
        from_clause += innertable

    # SELECT CLAUSE OF SUBQUERY -- apply DISTINCT and AGGREGATES
    select_clause = f"SELECT{' DISTINCT' if distinct else ''} "
    projection_column = subquery_column_alias
    if operator == AGGREGATE:
        select_clause += (
            f"{aggregate_function}({table_alias}."
            f"{correlated_column}) AS {projection_column}"
        )
    else:
        select_clause += f"{table_alias}.{correlated_column} AS {projection_column}"

    # WHERE CLAUSE OF SUBQUERY -- apply CORRELATION CONDITION if applicable
    where_clause = f"WHERE {table_alias}.a = {outertable}.a" if is_correlated else ""

    # GROUP BY CLAUSE OF SUBQUERY -- apply GROUP BY if applicable
    group_by_clause = "GROUP BY a" if group_by else ""

    # ORDER BY CLAUSE OF SUBQUERY -- ADD SORT IF THE OPERATOR IS AN ORDER-BY OR, THERE IS A LIMIT.
    requires_limit_one = subquery_type == SCALAR and (
        (operator == AGGREGATE and group_by is True)
        or operator != LIMIT
        or (operator == LIMIT and limit_value != 1)
    )
    order_by_clause = (
        f"ORDER BY {projection_column} DESC NULLS FIRST"
        if operator == ORDER_BY or requires_limit_one or operator == LIMIT
        else ""
    )

    # LIMIT CLAUSE OF SUBQUERY -- ADD LIMIT IF THE OPERATOR IS A LIMIT, OR IS REQUIRED FOR TEST
    # DETERMINISM.
    limit_clause = (
        "LIMIT 1"
        if requires_limit_one
        else (f"LIMIT {limit_value}" if operator == LIMIT else "")
    )

    sql_string = (
        f"({select_clause} {from_clause} {where_clause} "
        f"{group_by_clause} {order_by_clause} {limit_clause})"
    )
    return sql_string, projection_column


def generate_query(
    innertable,
    outertable,
    clause,
    subquery_type,
    is_correlated,
    distinct,
    subquery_operator,
    subquery_operator_specification,
):
    subquery_sql, subquery_projection_column = generate_subquery(
        innertable,
        outertable,
        subquery_type,
        is_correlated,
        distinct,
        subquery_operator,
        subquery_operator_specification,
    )

    select_clause, from_clause, where_clause, query_projection = "", "", "", []
    if clause == SELECT:
        query_projection = [
            f"{outertable}.{correlated_column}",
            f"{outertable}.{other_column}",
            subquery_alias,
        ]
        from_clause = f"FROM {outertable}"
        select_clause = (
            f"SELECT {outertable}.{correlated_column}, {outertable}.{other_column}, "
            f"{subquery_sql} AS subquery_alias"
        )
    elif clause == FROM:
        query_projection = [subquery_projection_column]
        select_clause = "SELECT " + ", ".join(query_projection)
        from_clause = f"FROM {subquery_sql} AS {subquery_alias}"
    elif clause == WHERE:
        query_projection = [
            f"{outertable}.{correlated_column}",
            f"{outertable}.{other_column}",
        ]
        select_clause = "SELECT " + ", ".join(query_projection)
        from_clause = f"FROM {outertable}"
        if subquery_type in [EXISTS, NOT_EXISTS]:
            where_clause = f"WHERE {subquery_type}{subquery_sql}"
        else:
            where_clause = f"WHERE {outertable}.a {subquery_type}{subquery_sql}"

    # Order by all projected columns for determinism
    order_by_clause = "ORDER BY " + ", ".join(
        [p + " NULLS FIRST" for p in query_projection]
    )
    complete_query = f"{select_clause} {from_clause} {where_clause} {order_by_clause};"

    comment_tags = [
        f"inner_table={innertable}",
        f"outer_table={outertable}",
        f"subquery_in={clause}",
        f"subquery_type={subquery_type if clause == WHERE else 'NA'}",
        f"is_correlated={is_correlated}",
        f"distinct={distinct}",
        f"subquery_operator={subquery_operator}",
        f"subquery_operator_specification={subquery_operator_specification}",
    ]
    comment = "-- " + ",".join(comment_tags) + "\n"

    return comment + complete_query


# ============================= SQL GENERATION =============================

if __name__ == "__main__":
    queries = []

    for input_table, output_table in table_combinations:
        for subquery_clause in subquery_clauses:
            subquery_type_choices = (
                [SCALAR] if subquery_clause == SELECT else subquery_types
            )
            for subquery_type in subquery_type_choices:
                correlated_choices = [False] if subquery_clause == FROM else correlated
                for is_correlated in correlated_choices:
                    for is_project_distinct in project_distinct:
                        for subquery_operator in subquery_operators:
                            for operator_type in subquery_operators[subquery_operator]:
                                generated_query = generate_query(
                                    input_table,
                                    output_table,
                                    subquery_clause,
                                    subquery_type,
                                    is_correlated,
                                    is_project_distinct,
                                    subquery_operator,
                                    operator_type,
                                )
                                if generated_query not in queries:
                                    queries.append(generated_query)

    result = table_creation_sql + "\n" + "\n".join(queries) + "\n"
    with open(
        "sql/core/src/test/resources/sql-tests/inputs/subquery/generated_subqueries_test.sql",
        "w",
    ) as file:
        file.write(result)
