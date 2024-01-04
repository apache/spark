inner_table = "inner_table"
outer_table = "outer_table"
no_match_inner_table = "no_match_inner_table"
no_match_outer_table = "no_match_outer_table"

table_creation = """CREATE TEMPORARY VIEW inner_table (a, b) AS VALUES
    (1, 10),
    (2, 20),
    (3, 30),
    (4, 40),
    (5, 50),
    (8, 80),
    (9, 90);
CREATE TEMPORARY VIEW outer_table (a, b) AS VALUES
    (1, 100),
    (2, 200),
    (3, 300),
    (4, 400),
    (6, 600),
    (7, 700),
    (10, 1000);
CREATE TEMPORARY VIEW no_match_inner_table (a, b) AS VALUES
    (6, 600),
    (7, 700),
    (10, 1000);
CREATE TEMPORARY VIEW no_match_outer_table (a, b) AS VALUES
    (5, 50),
    (8, 80),
    (9, 90);
"""

combination_of_tables = [
    (inner_table, outer_table),
    (no_match_inner_table, outer_table),
    (inner_table, no_match_outer_table),
    (no_match_inner_table, no_match_outer_table),
]

SELECT, FROM, WHERE, HAVING = "SELECT", "FROM", "WHERE", "HAVING"
subquery_clauses = [SELECT, FROM, WHERE, HAVING]

# Subquery type
IN, NOT_IN, EXISTS, NOT_EXISTS, SCALAR = "IN", "NOT IN", "EXISTS", "NOT EXISTS", "="
subquery_types = [IN, NOT_IN, EXISTS, NOT_EXISTS, SCALAR]

# Subquery properties - correlated or not
correlated = [True, False]

# Distinct projection or not
project_distinct = [True, False]

# Subquery operators
AGGREGATE, LIMIT, WINDOW, ORDER_BY = "AGGREGATE", "LIMIT", "WINDOW", "ORDER BY"
operators_within_subquery = [AGGREGATE, LIMIT, ORDER_BY]
aggregation_functions = ["SUM", "COUNT"]
# Can aggregate without GROUP BY
group_by = [True, False]
limit_values = [1, 10]


def generate_subquery(
        innertable,
        outertable,
        clause,
        subquery_type,
        is_correlated,
        distinct,
        subquery_operator,
        aggregate_function=None,
        group_by=None,
        limit_value=None,
):
    subquery_clause = "("
    subquery_projection = ""
    select_clause = f"SELECT {'DISTINCT' if distinct else ''}"
    if subquery_operator == AGGREGATE:
        subquery_projection = "c "
        subquery_clause += f"{select_clause} {aggregate_function}(a) AS {subquery_projection} FROM {innertable} "
    else:
        subquery_projection = "a "
        subquery_clause += f"{select_clause} {subquery_projection} FROM {innertable} "

    if is_correlated and clause != FROM:
        subquery_clause += f" WHERE {innertable}.a = {outertable}.a "

    # TODO: add window functions, joins, set operations
    if subquery_operator == AGGREGATE and group_by:
        # Must group by correlated column.
        subquery_clause += "GROUP BY a "

    # Scalar subquery to return only one row
    requires_limit = (subquery_type == SCALAR or clause == SELECT) and (
            (subquery_operator == AGGREGATE and group_by == True)
            or subquery_operator != LIMIT
            or (subquery_operator == LIMIT and limit_value != 1)
    )

    if subquery_operator == ORDER_BY or requires_limit:
        if subquery_operator == AGGREGATE:
            subquery_clause += f"ORDER BY {subquery_projection} DESC "
        else:
            subquery_clause += f"ORDER BY {subquery_projection} DESC "

    if requires_limit:
        subquery_clause += " LIMIT 1 "
    elif subquery_operator == LIMIT:
        subquery_clause += f"LIMIT {limit_value} "
    subquery_clause += ") "

    query = ""
    projection = ""
    if clause == SELECT:
        projection = f"{outertable}.b, subquery "
        query += (
            f"SELECT {outertable}.b, {subquery_clause} AS subquery FROM {outertable} "
        )
    elif clause == FROM:
        projection = subquery_projection
        query += f"SELECT {projection} FROM {subquery_clause} AS subquery "
    elif clause == WHERE:
        projection = "a, b "
        if subquery_type in [EXISTS, NOT_EXISTS]:
            query += f"SELECT {projection} FROM {outertable} WHERE {subquery_type}{subquery_clause} "
        else:
            query += f"SELECT {projection} FROM {outertable} WHERE {outertable}.a {subquery_type}{subquery_clause} "
    # TODO: add having

    # Order by all projected columns for determinism
    query += f" ORDER BY {projection};"

    has_limit = LIMIT in query
    comment_tags = [
        f"subquery_in={clause}",
        f"subquery_type={subquery_type}",
        f"is_correlated={is_correlated}",
        f"subquery_operator={subquery_operator}",
        f"distinct_projection={distinct}",
        f"aggregate_function(count_bug)={aggregate_function if subquery_operator == AGGREGATE else None}",
        f"group_by={group_by if subquery_operator == AGGREGATE else None}",
        f"has_limit={has_limit}",
    ]
    comment = "-- " + ",".join(comment_tags) + "\n"

    return comment + query


queries = []

for innertable, outertable in combination_of_tables:
    for clause in subquery_clauses:
        for subquery_type in subquery_types:
            for is_correlated in correlated:
                for subquery_operator in operators_within_subquery:
                    for aggregation_function in aggregation_functions:
                        for gb in group_by:
                            for limit_value in limit_values:
                                for distinct in project_distinct:
                                    query = generate_subquery(
                                        innertable,
                                        outertable,
                                        clause,
                                        subquery_type,
                                        is_correlated,
                                        distinct,
                                        subquery_operator,
                                        aggregation_function,
                                        gb,
                                        limit_value,
                                    )
                                    if query not in queries:
                                        queries.append(query)

queries = [query.strip() for query in queries if SELECT in query]

result = table_creation + "\n"
for q in queries:
    result += q + '\n'
    # print(q)
    # print()

with open("sql/core/src/test/resources/sql-tests/inputs/subquery/generated_subqueries_test.sql", "w") as file:
    file.write(result)
