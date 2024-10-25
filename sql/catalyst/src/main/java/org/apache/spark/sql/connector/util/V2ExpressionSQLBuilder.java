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
import java.util.Map;
import java.util.StringJoiner;

import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.connector.expressions.Cast;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Extract;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.UserDefinedScalarFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Avg;
import org.apache.spark.sql.connector.expressions.aggregate.Max;
import org.apache.spark.sql.connector.expressions.aggregate.Min;
import org.apache.spark.sql.connector.expressions.aggregate.Count;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.aggregate.GeneralAggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Sum;
import org.apache.spark.sql.connector.expressions.aggregate.UserDefinedAggregateFunc;
import org.apache.spark.sql.types.DataType;

/**
 * The builder to generate SQL from V2 expressions.
 *
 * @since 3.3.0
 */
public class V2ExpressionSQLBuilder {

  /**
   * Escape the special chars for like pattern.
   *
   * Note: This method adopts the escape representation within Spark and is not bound to any JDBC
   * dialect. JDBC dialect should overwrite this API if the underlying database have more special
   * chars other than _ and %.
   */
  protected String escapeSpecialCharsForLikePattern(String str) {
    StringBuilder builder = new StringBuilder();

    for (char c : str.toCharArray()) {
      switch (c) {
        case '_' -> builder.append("\\_");
        case '%' -> builder.append("\\%");
        default -> builder.append(c);
      }
    }

    return builder.toString();
  }

  public String build(Expression expr) {
    if (expr instanceof Literal literal) {
      return visitLiteral(literal);
    } else if (expr instanceof NamedReference namedReference) {
      return visitNamedReference(namedReference);
    } else if (expr instanceof Cast cast) {
      return visitCast(build(cast.expression()), cast.expressionDataType(), cast.dataType());
    } else if (expr instanceof Extract extract) {
      return visitExtract(extract.field(), build(extract.source()));
    } else if (expr instanceof SortOrder sortOrder) {
      return visitSortOrder(
        build(sortOrder.expression()), sortOrder.direction(), sortOrder.nullOrdering());
    } else if (expr instanceof GeneralScalarExpression e) {
      String name = e.name();
      return switch (name) {
        case "IN" -> {
          Expression[] expressions = e.children();
          List<String> children = expressionsToStringList(expressions, 1, expressions.length - 1);
          yield visitIn(build(expressions[0]), children);
        }
        case "IS_NULL" -> visitIsNull(build(e.children()[0]));
        case "IS_NOT_NULL" -> visitIsNotNull(build(e.children()[0]));
        case "STARTS_WITH" -> visitStartsWith(build(e.children()[0]), build(e.children()[1]));
        case "ENDS_WITH" -> visitEndsWith(build(e.children()[0]), build(e.children()[1]));
        case "CONTAINS" -> visitContains(build(e.children()[0]), build(e.children()[1]));
        case "=", "<>", "<=>", "<", "<=", ">", ">=" ->
          visitBinaryComparison(name, inputToSQL(e.children()[0]), inputToSQL(e.children()[1]));
        case "+", "*", "/", "%", "&", "|", "^" ->
          visitBinaryArithmetic(name, inputToSQL(e.children()[0]), inputToSQL(e.children()[1]));
        case "-" -> {
          if (e.children().length == 1) {
            yield visitUnaryArithmetic(name, inputToSQL(e.children()[0]));
          } else {
            yield visitBinaryArithmetic(
              name, inputToSQL(e.children()[0]), inputToSQL(e.children()[1]));
          }
        }
        case "AND" -> visitAnd(name, build(e.children()[0]), build(e.children()[1]));
        case "OR" -> visitOr(name, build(e.children()[0]), build(e.children()[1]));
        case "NOT" -> visitNot(build(e.children()[0]));
        case "~" -> visitUnaryArithmetic(name, inputToSQL(e.children()[0]));
        case "ABS", "COALESCE", "GREATEST", "LEAST", "RAND", "LOG", "LOG10", "LOG2", "LN", "EXP",
          "POWER", "SQRT", "FLOOR", "CEIL", "ROUND", "SIN", "SINH", "COS", "COSH", "TAN", "TANH",
          "COT", "ASIN", "ASINH", "ACOS", "ACOSH", "ATAN", "ATANH", "ATAN2", "CBRT", "DEGREES",
          "RADIANS", "SIGN", "WIDTH_BUCKET", "SUBSTRING", "UPPER", "LOWER", "TRANSLATE",
          "DATE_ADD", "DATE_DIFF", "TRUNC", "AES_ENCRYPT", "AES_DECRYPT", "SHA1", "SHA2", "MD5",
          "CRC32", "BIT_LENGTH", "CHAR_LENGTH", "CONCAT" ->
          visitSQLFunction(name, expressionsToStringArray(e.children()));
        case "CASE_WHEN" -> visitCaseWhen(expressionsToStringArray(e.children()));
        case "TRIM" -> visitTrim("BOTH", expressionsToStringArray(e.children()));
        case "LTRIM" -> visitTrim("LEADING", expressionsToStringArray(e.children()));
        case "RTRIM" -> visitTrim("TRAILING", expressionsToStringArray(e.children()));
        case "OVERLAY" -> visitOverlay(expressionsToStringArray(e.children()));
        // TODO supports other expressions
        default -> visitUnexpectedExpr(expr);
      };
    } else if (expr instanceof Min min) {
      return visitAggregateFunction("MIN", false,
        expressionsToStringArray(min.children()));
    } else if (expr instanceof Max max) {
      return visitAggregateFunction("MAX", false,
        expressionsToStringArray(max.children()));
    } else if (expr instanceof Count count) {
      return visitAggregateFunction("COUNT", count.isDistinct(),
        expressionsToStringArray(count.children()));
    } else if (expr instanceof Sum sum) {
      return visitAggregateFunction("SUM", sum.isDistinct(),
        expressionsToStringArray(sum.children()));
    } else if (expr instanceof CountStar) {
      return visitAggregateFunction("COUNT", false, new String[]{"*"});
    } else if (expr instanceof Avg avg) {
      return visitAggregateFunction("AVG", avg.isDistinct(),
        expressionsToStringArray(avg.children()));
    } else if (expr instanceof GeneralAggregateFunc f) {
      if (f.orderingWithinGroups().length == 0) {
        return visitAggregateFunction(f.name(), f.isDistinct(),
          expressionsToStringArray(f.children()));
      } else {
        return visitInverseDistributionFunction(
          f.name(),
          f.isDistinct(),
          expressionsToStringArray(f.children()),
          expressionsToStringArray(f.orderingWithinGroups()));
      }
    } else if (expr instanceof UserDefinedScalarFunc f) {
      return visitUserDefinedScalarFunction(f.name(), f.canonicalName(),
        expressionsToStringArray(f.children()));
    } else if (expr instanceof UserDefinedAggregateFunc f) {
      return visitUserDefinedAggregateFunction(f.name(), f.canonicalName(), f.isDistinct(),
        expressionsToStringArray(f.children()));
    } else {
      return visitUnexpectedExpr(expr);
    }
  }

  protected String visitLiteral(Literal<?> literal) {
    return literal.toString();
  }

  protected String visitNamedReference(NamedReference namedRef) {
    return namedRef.toString();
  }

  protected String visitIn(String v, List<String> list) {
    if (list.isEmpty()) {
      return "CASE WHEN " + v + " IS NULL THEN NULL ELSE FALSE END";
    }
    return joinListToString(list, ", ", v + " IN (", ")");
  }

  protected String visitIsNull(String v) {
    return v + " IS NULL";
  }

  protected String visitIsNotNull(String v) {
    return v + " IS NOT NULL";
  }

  protected String visitStartsWith(String l, String r) {
    // Remove quotes at the beginning and end.
    // e.g. converts "'str'" to "str".
    String value = r.substring(1, r.length() - 1);
    return l + " LIKE '" + escapeSpecialCharsForLikePattern(value) + "%' ESCAPE '\\'";
  }

  protected String visitEndsWith(String l, String r) {
    // Remove quotes at the beginning and end.
    // e.g. converts "'str'" to "str".
    String value = r.substring(1, r.length() - 1);
    return l + " LIKE '%" + escapeSpecialCharsForLikePattern(value) + "' ESCAPE '\\'";
  }

  protected String visitContains(String l, String r) {
    // Remove quotes at the beginning and end.
    // e.g. converts "'str'" to "str".
    String value = r.substring(1, r.length() - 1);
    return l + " LIKE '%" + escapeSpecialCharsForLikePattern(value) + "%' ESCAPE '\\'";
  }

  protected String inputToSQL(Expression input) {
    if (input.children().length > 1) {
      return "(" + build(input) + ")";
    } else {
      return build(input);
    }
  }

  protected String inputToCaseWhenSQL(Expression input) {
    return "CASE WHEN " + inputToSQL(input) + " THEN 1 ELSE 0 END";
  }

  protected String visitBinaryComparison(String name, String l, String r) {
    if (name.equals("<=>")) {
      return "((" + l + " IS NOT NULL AND " + r + " IS NOT NULL AND " + l + " = " + r + ") " +
              "OR (" + l + " IS NULL AND " + r + " IS NULL))";
    }
    return l + " " + name + " " + r;
  }

  protected String visitBinaryArithmetic(String name, String l, String r) {
    return l + " " + name + " " + r;
  }

  protected String visitCast(String expr, DataType exprDataType, DataType targetDataType) {
    return "CAST(" + expr + " AS " + targetDataType.typeName() + ")";
  }

  protected String visitAnd(String name, String l, String r) {
    return "(" + l + ") " + name + " (" + r + ")";
  }

  protected String visitOr(String name, String l, String r) {
    return "(" + l + ") " + name + " (" + r + ")";
  }

  protected String visitNot(String v) {
    return "NOT (" + v + ")";
  }

  protected String visitUnaryArithmetic(String name, String v) { return name + v; }

  protected String visitCaseWhen(String[] children) {
    StringBuilder sb = new StringBuilder("CASE");
    for (int i = 0; i < children.length; i += 2) {
      String c = children[i];
      int j = i + 1;
      if (j < children.length) {
        String v = children[j];
        sb.append(" WHEN ");
        sb.append(c);
        sb.append(" THEN ");
        sb.append(v);
      } else {
        sb.append(" ELSE ");
        sb.append(c);
      }
    }
    sb.append(" END");
    return sb.toString();
  }

  protected String visitSQLFunction(String funcName, String[] inputs) {
    return joinArrayToString(inputs, ", ", funcName + "(", ")");
  }

  protected String visitAggregateFunction(
      String funcName, boolean isDistinct, String[] inputs) {
    if (isDistinct) {
      return joinArrayToString(inputs, ", ", funcName + "(DISTINCT ", ")");
    } else {
      return joinArrayToString(inputs, ", ", funcName + "(", ")");
    }
  }

  protected String visitInverseDistributionFunction(
      String funcName, boolean isDistinct, String[] inputs, String[] orderingWithinGroups) {
    assert(isDistinct == false);
    String withinGroup =
      joinArrayToString(orderingWithinGroups, ", ", "WITHIN GROUP (ORDER BY ", ")");
    String functionCall = joinArrayToString(inputs, ", ", funcName + "(", ")");
    return functionCall + " " + withinGroup;
  }

  protected String visitUserDefinedScalarFunction(
      String funcName, String canonicalName, String[] inputs) {
    throw new SparkUnsupportedOperationException(
      "_LEGACY_ERROR_TEMP_3141",
      Map.of("class", this.getClass().getSimpleName(), "funcName", funcName));
  }

  protected String visitUserDefinedAggregateFunction(
      String funcName, String canonicalName, boolean isDistinct, String[] inputs) {
    throw new SparkUnsupportedOperationException(
      "_LEGACY_ERROR_TEMP_3142",
      Map.of("class", this.getClass().getSimpleName(), "funcName", funcName));
  }

  protected String visitUnexpectedExpr(Expression expr) throws IllegalArgumentException {
    throw new SparkIllegalArgumentException(
      "_LEGACY_ERROR_TEMP_3207", Map.of("expr", String.valueOf(expr)));
  }

  protected String visitOverlay(String[] inputs) {
    assert(inputs.length == 3 || inputs.length == 4);
    if (inputs.length == 3) {
      return "OVERLAY(" + inputs[0] + " PLACING " + inputs[1] + " FROM " + inputs[2] + ")";
    } else {
      return "OVERLAY(" + inputs[0] + " PLACING " + inputs[1] + " FROM " + inputs[2] +
        " FOR " + inputs[3]+ ")";
    }
  }

  protected String visitTrim(String direction, String[] inputs) {
    assert(inputs.length == 1 || inputs.length == 2);
    if (inputs.length == 1) {
      return "TRIM(" + direction + " FROM " + inputs[0] + ")";
    } else {
      return "TRIM(" + direction + " " + inputs[1] + " FROM " + inputs[0] + ")";
    }
  }

  protected String visitExtract(String field, String source) {
    return "EXTRACT(" + field + " FROM " + source + ")";
  }

  protected String visitSortOrder(
      String sortKey, SortDirection sortDirection, NullOrdering nullOrdering) {
    return sortKey + " " + sortDirection + " " + nullOrdering;
  }

  private String joinArrayToString(
      String[] inputs, CharSequence delimiter, CharSequence prefix, CharSequence suffix) {
    StringJoiner joiner = new StringJoiner(delimiter, prefix, suffix);
    for (String input : inputs) {
      joiner.add(input);
    }
    return joiner.toString();
  }

  private String joinListToString(
     List<String> inputs, CharSequence delimiter, CharSequence prefix, CharSequence suffix) {
    StringJoiner joiner = new StringJoiner(delimiter, prefix, suffix);
    for (String input : inputs) {
      joiner.add(input);
    }
    return joiner.toString();
  }

  protected String[] expressionsToStringArray(Expression[] expressions) {
    String[] result = new String[expressions.length];
    for (int i = 0; i < expressions.length; i++) {
      result[i] = build(expressions[i]);
    }
    return result;
  }

  private List<String> expressionsToStringList(Expression[] expressions, int offset, int length) {
    List<String> list = new ArrayList<>(length);
    final int till = Math.min(offset + length, expressions.length);
    while (offset < till) {
      list.add(build(expressions[offset]));
      offset++;
    }
    return list;
  }
}
