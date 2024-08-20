DROP VIEW IF EXISTS t1;
DROP VIEW IF EXISTS t2;
CREATE OR REPLACE TEMPORARY VIEW t1 AS VALUES (0, 1), (1, 2) t(c1, c2);
CREATE OR REPLACE TEMPORARY VIEW t2 AS VALUES (0, 1), (1, 2), (1, 3) t(partition_col, input);

-- test basic udtf
SELECT * FROM udtf(1, 2);
SELECT * FROM udtf(-1, 0);
SELECT * FROM udtf(0, -1);
SELECT * FROM udtf(0, 0);

-- test column alias
SELECT a, b FROM udtf(1, 2) t(a, b);

-- test lateral join
SELECT * FROM t1, LATERAL udtf(c1, c2);
SELECT * FROM t1 LEFT JOIN LATERAL udtf(c1, c2);
SELECT * FROM udtf(1, 2) t(c1, c2), LATERAL udtf(c1, c2);

-- test non-deterministic input
SELECT * FROM udtf(cast(rand(0) AS int) + 1, 1);

-- test UDTF calls that take input TABLE arguments
-- As a reminder, the UDTFCountSumLast function returns this analyze result:
--   AnalyzeResult(
--       schema=StructType()
--           .add("count", IntegerType())
--           .add("total", IntegerType())
--           .add("last", IntegerType()))
SELECT * FROM UDTFCountSumLast(TABLE(t2) WITH SINGLE PARTITION);
SELECT * FROM UDTFCountSumLast(TABLE(t2) PARTITION BY partition_col ORDER BY input);
SELECT * FROM UDTFCountSumLast(TABLE(t2) PARTITION BY partition_col ORDER BY input DESC);
SELECT * FROM
    VALUES (0), (1) AS t(col)
    JOIN LATERAL
    UDTFCountSumLast(TABLE(t2) PARTITION BY partition_col ORDER BY input DESC);

-- test UDTF calls that take input TABLE arguments and the 'analyze' method returns required
-- partitioning and/or ordering properties for Catalyst to enforce for the input table
-- As a reminder, the UDTFWithSinglePartition function returns this analyze result:
--     AnalyzeResult(
--           schema=StructType()
--               .add("count", IntegerType())
--               .add("total", IntegerType())
--               .add("last", IntegerType()),
--           with_single_partition=True,
--           order_by=[
--               OrderingColumn("input"),
--               OrderingColumn("partition_col")])
SELECT * FROM UDTFWithSinglePartition(0, TABLE(t2));
SELECT * FROM UDTFWithSinglePartition(1, TABLE(t2));
SELECT * FROM UDTFWithSinglePartition(0, TABLE(t2) WITH SINGLE PARTITION);
SELECT * FROM UDTFWithSinglePartition(0, TABLE(t2) PARTITION BY partition_col);
SELECT * FROM
    VALUES (0), (1) AS t(col)
    JOIN LATERAL
    UDTFWithSinglePartition(0, TABLE(t2) PARTITION BY partition_col);
-- As a reminder, the UDTFPartitionByOrderBy function returns this analyze result:
--     AnalyzeResult(
--         schema=StructType()
--             .add("partition_col", IntegerType())
--             .add("count", IntegerType())
--             .add("total", IntegerType())
--             .add("last", IntegerType()),
--         partition_by=[
--             PartitioningColumn("partition_col")
--         ],
--         order_by=[
--             OrderingColumn("input")
--         ])
SELECT * FROM UDTFPartitionByOrderBy(TABLE(t2));
SELECT * FROM UDTFPartitionByOrderBy(TABLE(t2) WITH SINGLE PARTITION);
SELECT * FROM UDTFPartitionByOrderBy(TABLE(t2) PARTITION BY partition_col);
SELECT * FROM
    VALUES (0), (1) AS t(col)
    JOIN LATERAL
    UDTFPartitionByOrderBy(TABLE(t2) PARTITION BY partition_col);
SELECT * FROM UDTFPartitionByOrderByComplexExpr(TABLE(t2));
SELECT * FROM UDTFPartitionByOrderBySelectExpr(TABLE(t2));
SELECT * FROM UDTFPartitionByOrderBySelectComplexExpr(TABLE(t2));
SELECT * FROM UDTFPartitionByOrderBySelectExprOnlyPartitionColumn(TABLE(t2));
SELECT * FROM UDTFInvalidSelectExprParseError(TABLE(t2));
SELECT * FROM UDTFInvalidSelectExprStringValue(TABLE(t2));
SELECT * FROM UDTFInvalidComplexSelectExprMissingAlias(TABLE(t2));
SELECT * FROM UDTFInvalidOrderByAscKeyword(TABLE(t2));
SELECT * FROM UDTFInvalidOrderByStringList(TABLE(t2));
-- As a reminder, UDTFInvalidPartitionByAndWithSinglePartition returns this analyze result:
--     AnalyzeResult(
--         schema=StructType()
--             .add("last", IntegerType()),
--         with_single_partition=True,
--         partition_by=[
--             PartitioningColumn("partition_col")
--         ])
SELECT * FROM UDTFInvalidPartitionByAndWithSinglePartition(TABLE(t2));
SELECT * FROM UDTFInvalidPartitionByAndWithSinglePartition(TABLE(t2) WITH SINGLE PARTITION);
SELECT * FROM UDTFInvalidPartitionByAndWithSinglePartition(TABLE(t2) PARTITION BY partition_col);
SELECT * FROM
    VALUES (0), (1) AS t(col)
    JOIN LATERAL
    UDTFInvalidPartitionByAndWithSinglePartition(TABLE(t2) PARTITION BY partition_col);
-- As a reminder, UDTFInvalidOrderByWithoutPartitionBy function returns this analyze result:
--     AnalyzeResult(
--         schema=StructType()
--             .add("last", IntegerType()),
--         order_by=[
--             OrderingColumn("input")
--         ])
SELECT * FROM UDTFInvalidOrderByWithoutPartitionBy(TABLE(t2));
SELECT * FROM UDTFInvalidOrderByWithoutPartitionBy(TABLE(t2) WITH SINGLE PARTITION);
SELECT * FROM UDTFInvalidOrderByWithoutPartitionBy(TABLE(t2) PARTITION BY partition_col);
SELECT * FROM
    VALUES (0), (1) AS t(col)
    JOIN LATERAL
    UDTFInvalidOrderByWithoutPartitionBy(TABLE(t2) PARTITION BY partition_col);
-- The following UDTF calls should fail because the UDTF's 'eval' or 'terminate' method returns None
-- to a non-nullable column, either directly or within an array/struct/map subfield.
SELECT * FROM InvalidEvalReturnsNoneToNonNullableColumnScalarType(TABLE(t2));
SELECT * FROM InvalidEvalReturnsNoneToNonNullableColumnArrayType(TABLE(t2));
SELECT * FROM InvalidEvalReturnsNoneToNonNullableColumnArrayElementType(TABLE(t2));
SELECT * FROM InvalidEvalReturnsNoneToNonNullableColumnStructType(TABLE(t2));
SELECT * FROM InvalidEvalReturnsNoneToNonNullableColumnMapType(TABLE(t2));
SELECT * FROM InvalidTerminateReturnsNoneToNonNullableColumnScalarType(TABLE(t2));
SELECT * FROM InvalidTerminateReturnsNoneToNonNullableColumnArrayType(TABLE(t2));
SELECT * FROM InvalidTerminateReturnsNoneToNonNullableColumnArrayElementType(TABLE(t2));
SELECT * FROM InvalidTerminateReturnsNoneToNonNullableColumnStructType(TABLE(t2));
SELECT * FROM InvalidTerminateReturnsNoneToNonNullableColumnMapType(TABLE(t2));
-- The following UDTF calls exercise various invalid function definitions and calls to show the
-- error messages.
SELECT * FROM UDTFForwardStateFromAnalyzeWithKwargs();
SELECT * FROM UDTFForwardStateFromAnalyzeWithKwargs(1, 2);
SELECT * FROM UDTFForwardStateFromAnalyzeWithKwargs(invalid => 2);
SELECT * FROM UDTFForwardStateFromAnalyzeWithKwargs(argument => 1, argument => 2);
SELECT * FROM InvalidAnalyzeMethodWithSinglePartitionNoInputTable(argument => 1);
SELECT * FROM InvalidAnalyzeMethodWithPartitionByNoInputTable(argument => 1);
SELECT * FROM InvalidAnalyzeMethodReturnsNonStructTypeSchema(TABLE(t2));
SELECT * FROM InvalidAnalyzeMethodWithPartitionByListOfStrings(argument => TABLE(t2));
SELECT * FROM InvalidForwardStateFromAnalyzeTooManyInitArgs(TABLE(t2));
SELECT * FROM InvalidNotForwardStateFromAnalyzeTooManyInitArgs(TABLE(t2));
SELECT * FROM UDTFWithSinglePartition(1);
SELECT * FROM UDTFWithSinglePartition(1, 2, 3);
SELECT * FROM UDTFWithSinglePartition(1, invalid_arg_name => 2);
SELECT * FROM UDTFWithSinglePartition(1, initial_count => 2);
SELECT * FROM UDTFWithSinglePartition(initial_count => 1, initial_count => 2);
SELECT * FROM UDTFInvalidPartitionByOrderByParseError(TABLE(t2));
-- Exercise the UDTF partitioning bug.
SELECT * FROM UDTFPartitionByIndexingBug(
    TABLE(
        SELECT
            5 AS unused_col,
            'hi' AS partition_col,
            1.0 AS double_col

        UNION ALL

        SELECT
            4 AS unused_col,
            'hi' AS partition_col,
            1.0 AS double_col
    )
);
-- Exercise a query with both a valid TABLE argument and an invalid unresolved column reference.
-- The 'eval' method of this UDTF would later throw an exception, but that is not relevant here
-- because the analysis of this query should fail before that point. We just want to make sure that
-- this analysis failure returns a reasonable error message.
SELECT * FROM
    InvalidEvalReturnsNoneToNonNullableColumnScalarType(TABLE(SELECT 1 AS X), unresolved_column);

-- cleanup
DROP VIEW t1;
DROP VIEW t2;
