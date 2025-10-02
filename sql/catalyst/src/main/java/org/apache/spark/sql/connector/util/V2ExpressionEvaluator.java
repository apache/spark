/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.BasePredicate;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Utility class for evaluating an {@link InternalRow} against a data source V2 {@link Predicate}.
 *
 * <p>This class provides methods to translate DSV2 predicates into Catalyst expressions based on a
 * given schema, and to evaluate these predicates against InternalRows.
 *
 * @since 4.1.0
 */
public final class V2ExpressionEvaluator {

  /**
   * Converts a Spark DataSourceV2 {@link Predicate} to a Catalyst {@link Expression}.
   *
   * <p>This method translates supported DSV2 predicates into their equivalent Catalyst expressions,
   * using the provided schema for column resolution. Unsupported predicates, or those referencing
   * unknown columns, will result in an empty Optional.
   *
   * <p>Supported predicates include:
   *
   * <ul>
   *   <li>Null tests: IS_NULL, IS_NOT_NULL
   *   <li>String functions: STARTS_WITH, ENDS_WITH, CONTAINS
   *   <li>IN operator
   *   <li>Comparison: =, >, >=, <, <=
   *   <li>Null-safe comparison: <=>
   *   <li>Logical operators: AND, OR, NOT
   *   <li>Constant predicates: ALWAYS_TRUE, ALWAYS_FALSE
   * </ul>
   *
   * @param predicate the DSV2 Predicate to convert
   * @param schema    the schema used for resolving column references
   * @return Catalyst Expression representing the converted predicate, or empty if the predicate is
   * unsupported or references unknown columns
   */
  public static Optional<Expression> dsv2PredicateToCatalystExpression(
      org.apache.spark.sql.connector.expressions.filter.Predicate predicate, StructType schema) {
    String predicateName = predicate.name();
    org.apache.spark.sql.connector.expressions.Expression[] children = predicate.children();

    switch (predicateName) {
      case "IS_NULL":
        if (children.length == 1) {
          Optional<Expression> expressionOpt =
              dsv2ExpressionToCatalystExpression(children[0], schema);
          if (expressionOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.IsNull(expressionOpt.get()));
          }
        }
        break;

      case "IS_NOT_NULL":
        if (children.length == 1) {
          Optional<Expression> expressionOpt =
              dsv2ExpressionToCatalystExpression(children[0], schema);
          if (expressionOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.IsNotNull(expressionOpt.get()));
          }
        }
        break;

      case "STARTS_WITH":
        if (children.length == 2) {
          Optional<Expression> leftOpt = dsv2ExpressionToCatalystExpression(children[0], schema);
          Optional<Expression> rightOpt = dsv2ExpressionToCatalystExpression(children[1], schema);
          if (leftOpt.isPresent() && rightOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.StartsWith(
                    leftOpt.get(), rightOpt.get()));
          }
        }
        break;

      case "ENDS_WITH":
        if (children.length == 2) {
          Optional<Expression> leftOpt = dsv2ExpressionToCatalystExpression(children[0], schema);
          Optional<Expression> rightOpt = dsv2ExpressionToCatalystExpression(children[1], schema);
          if (leftOpt.isPresent() && rightOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.EndsWith(
                    leftOpt.get(), rightOpt.get()));
          }
        }
        break;

      case "CONTAINS":
        if (children.length == 2) {
          Optional<Expression> leftOpt = dsv2ExpressionToCatalystExpression(children[0], schema);
          Optional<Expression> rightOpt = dsv2ExpressionToCatalystExpression(children[1], schema);
          if (leftOpt.isPresent() && rightOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.Contains(
                    leftOpt.get(), rightOpt.get()));
          }
        }
        break;

      case "IN":
        if (children.length >= 2) {
          Optional<Expression> firstOpt = dsv2ExpressionToCatalystExpression(children[0], schema);
          if (firstOpt.isPresent()) {
            List<Expression> values = new ArrayList<>();
            for (int i = 1; i < children.length; i++) {
              Optional<Expression> valueOpt =
                  dsv2ExpressionToCatalystExpression(children[i], schema);
              if (valueOpt.isPresent()) {
                values.add(valueOpt.get());
              } else {
                // if any value in the IN list cannot be converted, return empty
                return Optional.empty();
              }
            }
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.In(
                    firstOpt.get(), CollectionConverters.asScala(values).toSeq()
                ));
          }
        }
        break;

      case "=":
        if (children.length == 2) {
          Optional<Expression> leftOpt = dsv2ExpressionToCatalystExpression(children[0], schema);
          Optional<Expression> rightOpt = dsv2ExpressionToCatalystExpression(children[1], schema);
          if (leftOpt.isPresent() && rightOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.EqualTo(
                    leftOpt.get(), rightOpt.get()));
          }
        }
        break;

      case "<>":
        if (children.length == 2) {
          Optional<Expression> leftOpt = dsv2ExpressionToCatalystExpression(children[0], schema);
          Optional<Expression> rightOpt = dsv2ExpressionToCatalystExpression(children[1], schema);
          if (leftOpt.isPresent() && rightOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.Not(
                    new org.apache.spark.sql.catalyst.expressions.EqualTo(
                        leftOpt.get(), rightOpt.get())));
          }
        }
        break;

      case "<=>":
        if (children.length == 2) {
          Optional<Expression> leftOpt = dsv2ExpressionToCatalystExpression(children[0], schema);
          Optional<Expression> rightOpt = dsv2ExpressionToCatalystExpression(children[1], schema);
          if (leftOpt.isPresent() && rightOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.EqualNullSafe(
                    leftOpt.get(), rightOpt.get()));
          }
        }
        break;

      case "<":
        if (children.length == 2) {
          Optional<Expression> leftOpt = dsv2ExpressionToCatalystExpression(children[0], schema);
          Optional<Expression> rightOpt = dsv2ExpressionToCatalystExpression(children[1], schema);
          if (leftOpt.isPresent() && rightOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.LessThan(
                    leftOpt.get(), rightOpt.get()));
          }
        }
        break;

      case "<=":
        if (children.length == 2) {
          Optional<Expression> leftOpt = dsv2ExpressionToCatalystExpression(children[0], schema);
          Optional<Expression> rightOpt = dsv2ExpressionToCatalystExpression(children[1], schema);
          if (leftOpt.isPresent() && rightOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.LessThanOrEqual(
                    leftOpt.get(), rightOpt.get()));
          }
        }
        break;

      case ">":
        if (children.length == 2) {
          Optional<Expression> leftOpt = dsv2ExpressionToCatalystExpression(children[0], schema);
          Optional<Expression> rightOpt = dsv2ExpressionToCatalystExpression(children[1], schema);
          if (leftOpt.isPresent() && rightOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.GreaterThan(
                    leftOpt.get(), rightOpt.get()));
          }
        }
        break;

      case ">=":
        if (children.length == 2) {
          Optional<Expression> leftOpt = dsv2ExpressionToCatalystExpression(children[0], schema);
          Optional<Expression> rightOpt = dsv2ExpressionToCatalystExpression(children[1], schema);
          if (leftOpt.isPresent() && rightOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual(
                    leftOpt.get(), rightOpt.get()));
          }
        }
        break;

      case "AND":
        if (children.length == 2) {
          Optional<Expression> leftOpt =
              dsv2PredicateToCatalystExpression(
                  (org.apache.spark.sql.connector.expressions.filter.Predicate)
                      predicate.children()[0],
                  schema);
          Optional<Expression> rightOpt =
              dsv2PredicateToCatalystExpression(
                  (org.apache.spark.sql.connector.expressions.filter.Predicate)
                      predicate.children()[1],
                  schema);
          if (leftOpt.isPresent() && rightOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.And(leftOpt.get(), rightOpt.get()));
          }
        }
        break;

      case "OR":
        if (children.length == 2) {
          Optional<Expression> leftOpt =
              dsv2PredicateToCatalystExpression(
                  (org.apache.spark.sql.connector.expressions.filter.Predicate)
                      predicate.children()[0],
                  schema);
          Optional<Expression> rightOpt =
              dsv2PredicateToCatalystExpression(
                  (org.apache.spark.sql.connector.expressions.filter.Predicate)
                      predicate.children()[1],
                  schema);
          if (leftOpt.isPresent() && rightOpt.isPresent()) {
            return Optional.of(
                new org.apache.spark.sql.catalyst.expressions.Or(leftOpt.get(), rightOpt.get()));
          }
        }
        break;

      case "NOT":
        if (children.length == 1) {
          Optional<Expression> childOpt =
              dsv2PredicateToCatalystExpression(
                  (org.apache.spark.sql.connector.expressions.filter.Predicate)
                      predicate.children()[0],
                  schema);
          if (childOpt.isPresent()) {
            return Optional.of(new org.apache.spark.sql.catalyst.expressions.Not(childOpt.get()));
          }
        }
        break;

      case "ALWAYS_TRUE":
        if (children.length == 0) {
          return Optional.of(
              org.apache.spark.sql.catalyst.expressions.Literal.create(
                  true, org.apache.spark.sql.types.DataTypes.BooleanType));
        }
        break;

      case "ALWAYS_FALSE":
        if (children.length == 0) {
          return Optional.of(
              org.apache.spark.sql.catalyst.expressions.Literal.create(
                  false, org.apache.spark.sql.types.DataTypes.BooleanType));
        }
        break;
    }

    return Optional.empty();
  }

  /**
   * Translate a DSV2 Expression to a Catalyst {@link Expression} using the provided schema.
   *
   * <p>This method handles NamedReference and LiteralValue expressions. NamedReferences are
   * resolved to BoundReferences based on the schema, while LiteralValues are converted to Catalyst
   * Literals. Unsupported expression types or references to unknown columns will result in an empty
   * Optional.
   *
   * @param expr   the DSV2 Expression to resolve
   * @param schema the schema used for resolving column references
   * @return Catalyst Expression representing the resolved expression, or empty if the expression is
   * unsupported or references unknown columns
   */
  public static Optional<Expression> dsv2ExpressionToCatalystExpression(
      org.apache.spark.sql.connector.expressions.Expression expr, StructType schema) {
    if (expr instanceof NamedReference ref) {
      String columnName = ref.fieldNames()[0];
      try {
        int index = schema.fieldIndex(columnName);
        StructField field = schema.fields()[index];
        return Optional.of(new BoundReference(index, field.dataType(), field.nullable()));
      } catch (IllegalArgumentException e) {
        // schema.fieldIndex(columnName) throws IllegalArgumentException if a field with the given
        // name does not exist
        return Optional.empty();
      }
    } else if (expr instanceof LiteralValue<?> literal) {
      return Optional.of(
          org.apache.spark.sql.catalyst.expressions.Literal.create(
              literal.value(), literal.dataType()));
    } else {
      return Optional.empty();
    }
  }


  /**
   * Evaluates a DSV2 {@link Predicate} on an {@link InternalRow} of the provided schema.
   *
   * <p>This method first converts the DSV2 Predicate to a Catalyst Expression using the provided
   * schema. If the conversion is successful, it creates a Predicate evaluator and evaluates it
   * against the given InternalRow. If the predicate cannot be converted, an empty Optional is
   * returned.
   *
   * @param predicate   the DSV2 Predicate to evaluate
   * @param internalRow the InternalRow to evaluate the predicate against
   * @param schema      the schema used for resolving column references in the predicate
   * @return Optional containing the result of the evaluation (true or false), or empty if the
   * predicate could not be converted
   */
  public static Optional<Boolean> evaluateInternalRowOnDsv2Predicate(
      org.apache.spark.sql.connector.expressions.filter.Predicate predicate,
      InternalRow internalRow,
      StructType schema) {
    Optional<Expression> catalystExpr = dsv2PredicateToCatalystExpression(predicate, schema);
    if (catalystExpr.isEmpty()) {
      return Optional.empty();
    }
    BasePredicate evaluator =
        org.apache.spark.sql.catalyst.expressions.Predicate.create(catalystExpr.get());
    return Optional.of(evaluator.eval(internalRow));
  }
}
