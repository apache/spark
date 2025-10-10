package org.apache.spark.sql.connector.util;

import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import static org.apache.spark.sql.connector.util.V2ExpressionEvaluator.convertV2PredicateToCatalyst;
import static org.apache.spark.sql.connector.util.V2ExpressionEvaluator.evaluateInternalRowOnDsv2Predicate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.SparkRuntimeException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;

public class V2ExpressionEvaluatorSuite {

  private final org.apache.spark.sql.types.StructType testSchema =
      new org.apache.spark.sql.types.StructType()
          .add("id", org.apache.spark.sql.types.DataTypes.IntegerType, false)
          .add("name", org.apache.spark.sql.types.DataTypes.StringType, true)
          .add("age", org.apache.spark.sql.types.DataTypes.IntegerType, true);

  private final InternalRow[] testData = new InternalRow[]{
      new GenericInternalRow(new Object[]{1, org.apache.spark.unsafe.types.UTF8String.fromString("Alice"), 30}),
      new GenericInternalRow(new Object[]{2, org.apache.spark.unsafe.types.UTF8String.fromString("Bob"), null}),
      new GenericInternalRow(new Object[]{3, null, 25}),
      new GenericInternalRow(new Object[]{4, org.apache.spark.unsafe.types.UTF8String.fromString("David"), 35})
  };

  private final NamedReference idRef = FieldReference.apply("id");
  private final NamedReference nameRef = FieldReference.apply("name");
  private final NamedReference ageRef = FieldReference.apply("age");

  private final org.apache.spark.sql.connector.expressions.filter.Predicate isNullPredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          "IS_NULL", new org.apache.spark.sql.connector.expressions.Expression[]{nameRef});
  private final org.apache.spark.sql.connector.expressions.filter.Predicate isNotNullPredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          "IS_NOT_NULL", new org.apache.spark.sql.connector.expressions.Expression[]{nameRef});
  private final org.apache.spark.sql.connector.expressions.filter.Predicate inPredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          "IN",
          new org.apache.spark.sql.connector.expressions.Expression[]{
              idRef,
              LiteralValue.apply(1, org.apache.spark.sql.types.DataTypes.IntegerType),
              LiteralValue.apply(3, org.apache.spark.sql.types.DataTypes.IntegerType)});
  private final org.apache.spark.sql.connector.expressions.filter.Predicate equalsPredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          "=",
          new org.apache.spark.sql.connector.expressions.Expression[]{
              idRef,
              LiteralValue.apply(2, org.apache.spark.sql.types.DataTypes.IntegerType)});
  private final org.apache.spark.sql.connector.expressions.filter.Predicate greaterThanPredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          ">",
          new org.apache.spark.sql.connector.expressions.Expression[]{
              ageRef,
              LiteralValue.apply(20, org.apache.spark.sql.types.DataTypes.IntegerType)});
  private final org.apache.spark.sql.connector.expressions.filter.Predicate notPredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          "NOT",
          new org.apache.spark.sql.connector.expressions.Expression[]{isNullPredicate});
  private final org.apache.spark.sql.connector.expressions.filter.Predicate andPredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          "AND",
          new org.apache.spark.sql.connector.expressions.Expression[]{isNotNullPredicate, greaterThanPredicate});
  private final org.apache.spark.sql.connector.expressions.filter.Predicate orPredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          "OR",
          new org.apache.spark.sql.connector.expressions.Expression[]{isNullPredicate, equalsPredicate});
  private final org.apache.spark.sql.connector.expressions.filter.Predicate unsupportedPredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          "UNSUPPORTED_OP",
          new org.apache.spark.sql.connector.expressions.Expression[]{idRef});

  @Test
  public void testV2ExpressionEvaluator() throws Throwable {
    // Null tests
    checkExpressionConversionAndEvaluation(isNullPredicate, true, new Boolean[]{false, false, true, false});
    checkExpressionConversionAndEvaluation(isNotNullPredicate, true, new Boolean[]{true, true, false, true});

    // IN operator
    checkExpressionConversionAndEvaluation(inPredicate, true, new Boolean[]{true, false, true, false});

    // Comparison operators
    checkExpressionConversionAndEvaluation(equalsPredicate, true, new Boolean[]{false, true, false, false});
    checkExpressionConversionAndEvaluation(greaterThanPredicate, true, new Boolean[]{true, false, true, true});

    // Logical operators
    checkExpressionConversionAndEvaluation(notPredicate, true, new Boolean[]{true, true, false, true});
    // AND: name IS NOT NULL AND age > 20
    // Row 0: Alice, 30 -> true AND true = true
    // Row 1: Bob, null -> true AND false = false
    // Row 2: null, 25 -> false AND true = false
    // Row 3: David, 35 -> true AND true = true
    checkExpressionConversionAndEvaluation(andPredicate, true, new Boolean[]{true, false, false, true});
    // OR: name IS NULL OR id = 2
    // Row 0: false OR false = false
    // Row 1: false OR true = true
    // Row 2: true OR false = true
    // Row 3: false OR false = false
    checkExpressionConversionAndEvaluation(orPredicate, true, new Boolean[]{false, true, true, false});

    // Unsupported predicate
    checkExpressionConversionAndEvaluation(unsupportedPredicate, false, null);
  }

  private void checkExpressionConversionAndEvaluation(
      org.apache.spark.sql.connector.expressions.filter.Predicate predicate,
      boolean isConvertible,
      Boolean[] expectedResults) throws Throwable {
    Optional<Expression> catalystExpr = convertV2PredicateToCatalyst(predicate, testSchema);

    if (isConvertible) {
      assertTrue(catalystExpr.isPresent(), "Predicate should be convertible");
      for (int i = 0; i < testData.length; i++) {
        Boolean evalResult = evaluateInternalRowOnDsv2Predicate(predicate, testData[i], testSchema, true);
        assertEquals(evalResult, expectedResults[i], String.format("Row %d: expected %s but got %s", i, expectedResults[i], evalResult));
      }
    } else {
      assertTrue(catalystExpr.isEmpty(), "Predicate should not be convertible");
      for (InternalRow row : testData) {
        Boolean evalResult = evaluateInternalRowOnDsv2Predicate(predicate, row, testSchema, true);
        assertTrue(evalResult, "Unconvertible predicate should return true when 'alwaysTrueOnUnconverted' is true");
      }
    }
  }

  @Test
  public void testV2ExpressionEvaluatorThrowException() {
    SparkRuntimeException e = Assertions.assertThrows(SparkRuntimeException.class,
      () -> evaluateInternalRowOnDsv2Predicate(
              unsupportedPredicate, testData[0], testSchema, false));
    Assertions.assertEquals("FAILED_TO_EVALUATE_DSV2_PREDICATE", e.getCondition());
    Assertions.assertTrue(e.getMessage().contains("UNSUPPORTED_OP"));
  }
}
