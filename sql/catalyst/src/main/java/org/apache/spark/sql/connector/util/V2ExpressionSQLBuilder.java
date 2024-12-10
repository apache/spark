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
import java.util.StringJoiner;

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
        case '_':
          builder.append("\\_");
          break;
        case '%':
          builder.append("\\%");
          break;
        default:
          builder.append(c);
      }
    }

    return builder.toString();
  }

  public String build(Expression expr) {
    if (expr instanceof Literal) {
      return visitLiteral((Literal<?>) expr);
    } else if (expr instanceof NamedReference) {
      return visitNamedReference((NamedReference) expr);
    } else if (expr instanceof Cast) {
      Cast cast = (Cast) expr;
      return visitCast(build(cast.expression()), cast.dataType());
    } else if (expr instanceof Extract) {
      Extract extract = (Extract) expr;
      return visitExtract(extract.field(), build(extract.source()));
    } else if (expr instanceof SortOrder) {
      SortOrder sortOrder = (SortOrder) expr;
      return visitSortOrder(
        build(sortOrder.expression()), sortOrder.direction(), sortOrder.nullOrdering());
    } else if (expr instanceof GeneralScalarExpression) {
      GeneralScalarExpression e = (GeneralScalarExpression) expr;
      String name = e.name();
      switch (name) {
        case "IN": {
          Expression[] expressions = e.children();
          List<String> children = expressionsToStringList(expressions, 1, expressions.length - 1);
          return visitIn(build(expressions[0]), children);
        }
        case "IS_NULL":
          return visitIsNull(build(e.children()[0]));
        case "IS_NOT_NULL":
          return visitIsNotNull(build(e.children()[0]));
        case "STARTS_WITH":
          return visitStartsWith(build(e.children()[0]), build(e.children()[1]));
        case "ENDS_WITH":
          return visitEndsWith(build(e.children()[0]), build(e.children()[1]));
        case "CONTAINS":
          return visitContains(build(e.children()[0]), build(e.children()[1]));
        case "=":
        case "<>":
        case "<=>":
        case "<":
        case "<=":
        case ">":
        case ">=":
          return visitBinaryComparison(
            name, inputToSQL(e.children()[0]), inputToSQL(e.children()[1]));
        case "+":
        case "*":
        case "/":
        case "%":
        case "&":
        case "|":
        case "^":
          return visitBinaryArithmetic(
            name, inputToSQL(e.children()[0]), inputToSQL(e.children()[1]));
        case "-":
          if (e.children().length == 1) {
            return visitUnaryArithmetic(name, inputToSQL(e.children()[0]));
          } else {
            return visitBinaryArithmetic(
              name, inputToSQL(e.children()[0]), inputToSQL(e.children()[1]));
          }
        case "AND":
          return visitAnd(name, build(e.children()[0]), build(e.children()[1]));
        case "OR":
          return visitOr(name, build(e.children()[0]), build(e.children()[1]));
        case "NOT":
          return visitNot(build(e.children()[0]));
        case "~":
          return visitUnaryArithmetic(name, inputToSQL(e.children()[0]));
        case "ABS":
        case "COALESCE":
        case "GREATEST":
        case "LEAST":
        case "RAND":
        case "LOG":
        case "LOG10":
        case "LOG2":
        case "LN":
        case "EXP":
        case "POWER":
        case "SQRT":
        case "FLOOR":
        case "CEIL":
        case "ROUND":
        case "SIN":
        case "SINH":
        case "COS":
        case "COSH":
        case "TAN":
        case "TANH":
        case "COT":
        case "ASIN":
        case "ASINH":
        case "ACOS":
        case "ACOSH":
        case "ATAN":
        case "ATANH":
        case "ATAN2":
        case "CBRT":
        case "DEGREES":
        case "RADIANS":
        case "SIGN":
        case "WIDTH_BUCKET":
        case "SUBSTRING":
        case "UPPER":
        case "LOWER":
        case "TRANSLATE":
        case "DATE_ADD":
        case "DATE_DIFF":
        case "TRUNC":
        case "AES_ENCRYPT":
        case "AES_DECRYPT":
        case "SHA1":
        case "SHA2":
        case "MD5":
        case "CRC32":
        case "BIT_LENGTH":
        case "CHAR_LENGTH":
        case "CONCAT":
          return visitSQLFunction(name, expressionsToStringArray(e.children()));
        case "CASE_WHEN": {
          return visitCaseWhen(expressionsToStringArray(e.children()));
        }
        case "TRIM":
          return visitTrim("BOTH", expressionsToStringArray(e.children()));
        case "LTRIM":
          return visitTrim("LEADING", expressionsToStringArray(e.children()));
        case "RTRIM":
          return visitTrim("TRAILING", expressionsToStringArray(e.children()));
        case "OVERLAY":
          return visitOverlay(expressionsToStringArray(e.children()));
        // TODO supports other expressions
        default:
          return visitUnexpectedExpr(expr);
      }
    } else if (expr instanceof Min) {
      Min min = (Min) expr;
      return visitAggregateFunction("MIN", false,
        expressionsToStringArray(min.children()));
    } else if (expr instanceof Max) {
      Max max = (Max) expr;
      return visitAggregateFunction("MAX", false,
        expressionsToStringArray(max.children()));
    } else if (expr instanceof Count) {
      Count count = (Count) expr;
      return visitAggregateFunction("COUNT", count.isDistinct(),
        expressionsToStringArray(count.children()));
    } else if (expr instanceof Sum) {
      Sum sum = (Sum) expr;
      return visitAggregateFunction("SUM", sum.isDistinct(),
        expressionsToStringArray(sum.children()));
    } else if (expr instanceof CountStar) {
      return visitAggregateFunction("COUNT", false, new String[]{"*"});
    } else if (expr instanceof Avg) {
      Avg avg = (Avg) expr;
      return visitAggregateFunction("AVG", avg.isDistinct(),
        expressionsToStringArray(avg.children()));
    } else if (expr instanceof GeneralAggregateFunc) {
      GeneralAggregateFunc f = (GeneralAggregateFunc) expr;
      return visitAggregateFunction(f.name(), f.isDistinct(),
        expressionsToStringArray(f.children()));
    } else if (expr instanceof UserDefinedScalarFunc) {
      UserDefinedScalarFunc f = (UserDefinedScalarFunc) expr;
      return visitUserDefinedScalarFunction(f.name(), f.canonicalName(),
        expressionsToStringArray(f.children()));
    } else if (expr instanceof UserDefinedAggregateFunc) {
      UserDefinedAggregateFunc f = (UserDefinedAggregateFunc) expr;
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

  protected String visitBinaryComparison(String name, String l, String r) {
    switch (name) {
      case "<=>":
        return "(" + l + " = " + r + ") OR (" + l + " IS NULL AND " + r + " IS NULL)";
      default:
        return l + " " + name + " " + r;
    }
  }

  protected String visitBinaryArithmetic(String name, String l, String r) {
    return l + " " + name + " " + r;
  }

  protected String visitCast(String l, DataType dataType) {
    return "CAST(" + l + " AS " + dataType.typeName() + ")";
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

  protected String visitUserDefinedScalarFunction(
      String funcName, String canonicalName, String[] inputs) {
    throw new UnsupportedOperationException(
      this.getClass().getSimpleName() + " does not support user defined function: " + funcName);
  }

  protected String visitUserDefinedAggregateFunction(
      String funcName, String canonicalName, boolean isDistinct, String[] inputs) {
    throw new UnsupportedOperationException(this.getClass().getSimpleName() +
      " does not support user defined aggregate function: " + funcName);
  }

  protected String visitUnexpectedExpr(Expression expr) throws IllegalArgumentException {
    throw new IllegalArgumentException("Unexpected V2 expression: " + expr);
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

  private String[] expressionsToStringArray(Expression[] expressions) {
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
