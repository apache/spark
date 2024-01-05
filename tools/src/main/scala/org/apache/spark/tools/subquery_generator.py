# ============================= VARIABLE DEFINITIONS =============================
correlated_column, other_column = "a", "b"
subquery_alias = "subquery_alias"
set_operation_alias = "set_alias"
aggregate_function_column_alias = "c"

inner_table = "inner_table"
outer_table = "outer_table"
# No matching rows for correlated column with outer tables
no_match_inner_table = "no_match_inner_table"
# No matching rows for correlated column with inner tables
no_match_outer_table = "no_match_outer_table"
# Used for join and set operations
join_table = "join_table"

# Query clauses
SELECT, FROM, WHERE = "SELECT", "FROM", "WHERE"

# Subquery type
IN, NOT_IN, EXISTS, NOT_EXISTS, SCALAR = "IN", "NOT IN", "EXISTS", "NOT EXISTS", "="

# Aggregate function types
SUM, COUNT = "SUM", "COUNT"

# Join types
INNER, LEFT_OUTER, RIGHT_OUTER = "INNER JOIN", "LEFT OUTER JOIN", "RIGHT OUTER JOIN"

# Set operations
INTERSECT, UNION, EXCEPT = "INTERSECT", "UNION", "EXCEPT"

# ============================= SUBQUERY VARIATIONS =============================

table_combinations = [
    (inner_table, outer_table),
    (no_match_inner_table, outer_table),
    (inner_table, no_match_outer_table),
    (no_match_inner_table, no_match_outer_table),
]

# Subquery types
subquery_types = [IN, NOT_IN, EXISTS, NOT_EXISTS, SCALAR]

# Subquery properties - correlated or not
correlated = [True, False]

# Distinct projection or not
project_distinct = [True, False]

# Query clause in which subquery is in.
# TODO: support HAVING operations
subquery_sqls = [SELECT, FROM, WHERE]

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

# Tuples of (limit, limitValue)
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
    clause,
    subquery_type,
    is_correlated,
    distinct,
    subquery_operator,
    subquery_operator_type,
):
    aggregate_function = (
        subquery_operator_type[0] if subquery_operator == AGGREGATE else None
    )
    group_by = subquery_operator_type[1] if subquery_operator == AGGREGATE else None
    limit_value = subquery_operator_type if subquery_operator == LIMIT else None

    # SELECT CLAUSE OF SUBQUERY -- apply DISTINCT and AGGREGATES
    subquery_select_clause = f"SELECT{' DISTINCT' if distinct else ''} "

    subquery_projection = ""
    subquery_from_alias = set_operation_alias if subquery_operator == SET_OP else innertable
    if subquery_operator == AGGREGATE:
        subquery_projection = aggregate_function_column_alias
        # TODO: Use correlated column?
        subquery_select_clause += f"{aggregate_function}({subquery_from_alias}.{correlated_column}) AS {subquery_projection}"
    else:
        # TODO: Use correlated column?
        subquery_projection = f'{subquery_from_alias}.{correlated_column}'
        subquery_select_clause += f"{subquery_projection}"

    # FROM CLAUSE OF SUBQUERY - apply JOINS and SET OPERATIONS
    subquery_from_clause = "FROM "
    if subquery_operator == SET_OP:
        subquery_from_clause += f"(SELECT a, b FROM {innertable} {subquery_operator_type} SELECT a, b FROM {join_table}) AS {subquery_from_alias}"
    elif subquery_operator == JOIN:
        subquery_from_clause += f"{innertable} {subquery_operator_type} {join_table} ON {innertable}.b = {join_table}.b)"
    else:
        subquery_from_clause += innertable

    # WHERE CLAUSE OF SUBQUERY
    subquery_where_clause = ""
    if is_correlated and clause != FROM:
        subquery_where_clause += f"WHERE {innertable}.a = {outertable}.a"

    # GROUP BY CLAUSE OF SUBQUERY
    subquery_group_by_clause = ""
    if subquery_operator == AGGREGATE and group_by:
        # Must group by correlated column.
        subquery_group_by_clause = "GROUP BY a"

    requires_limit_one = (subquery_type == SCALAR or clause == SELECT) and (
        (subquery_operator == AGGREGATE and group_by is True)
        or subquery_operator != LIMIT
        or (subquery_operator == LIMIT and limit_value != 1)
    )

    subquery_order_by_clause = ""
    # ORDER BY CLAUSE OF SUBQUERY
    if (
        subquery_operator == ORDER_BY
        or requires_limit_one
        or subquery_operator == LIMIT
    ):
        subquery_order_by_clause = f"ORDER BY {subquery_projection} DESC"

    # LIMIT CLAUSE OF SUBQUERY
    subquery_limit_clause = ""
    if requires_limit_one:
        subquery_limit_clause = "LIMIT 1"
    elif subquery_operator == LIMIT:
        subquery_limit_clause = f"LIMIT {limit_value}"

    subquery_sql = f"({subquery_select_clause} {subquery_from_clause} {subquery_where_clause} {subquery_group_by_clause} {subquery_order_by_clause} {subquery_limit_clause})"

    query = ""
    query_projection = ""
    if clause == SELECT:
        query_projection = f"{outertable}.b, {subquery_alias}"
        query += f"SELECT {outertable}.b, {subquery_sql} AS {subquery_alias} FROM {outertable}"
    elif clause == FROM:
        query_projection = subquery_projection
        query += f"SELECT {query_projection} FROM {subquery_sql} AS {subquery_alias}"
    elif clause == WHERE:
        query_projection = "a, b"
        if subquery_type in [EXISTS, NOT_EXISTS]:
            query += f"SELECT {query_projection} FROM {outertable} WHERE {subquery_type}{subquery_sql}"
        else:
            query += f"SELECT {query_projection} FROM {outertable} WHERE {outertable}.a {subquery_type}{subquery_sql}"

    # Order by all projected columns for determinism
    query += f" ORDER BY {query_projection};"

    has_limit = LIMIT in query
    comment_tags = [
        f"subquery_in={clause}",
        f"subquery_type={subquery_type if clause == WHERE else 'NA'}",
        f"is_correlated={is_correlated}",
        f"distinct={distinct}",
        f"subquery_operator={subquery_operator}",
        f"subquery_operator_type={subquery_operator_type}",
    ]
    comment = "-- " + ",".join(comment_tags) + "\n"

    return comment + query


# ============================= SQL GENERATION =============================

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
CREATE TEMPORARY VIEW {no_match_inner_table} (a, b) AS VALUES
    (5, 50),
    (8, 80),
    (9, 90);
CREATE TEMPORARY VIEW {no_match_outer_table} (a, b) AS VALUES
    (6, 600),
    (7, 700),
    (10, 1000);
CREATE TEMPORARY VIEW {join_table} (a, b) AS VALUES
    (1, 10),
    (3, 30),
    (4, 400),
    (6, 600),
    (8, 80);
"""

if __name__ == "__main__":
    queries = []

    for itble, otble in table_combinations:
        for cl in subquery_sqls:
            for st in subquery_types:
                for ic in correlated:
                    for d in project_distinct:
                        for so in subquery_operators:
                            for ot in subquery_operators[so]:
                                query = generate_subquery(
                                    itble,
                                    otble,
                                    cl,
                                    st,
                                    ic,
                                    d,
                                    so,
                                    ot,
                                )
                                if query not in queries:
                                    queries.append(query)

    queries = [query.strip() for query in queries if SELECT in query]

    result = table_creation_sql + "\n"
    for q in queries:
        result += q + "\n"
        # print(q)
        # print()

    with open(
        "sql/core/src/test/resources/sql-tests/inputs/subquery/generated_subqueries_test.sql",
        "w",
    ) as file:
        file.write(result)
