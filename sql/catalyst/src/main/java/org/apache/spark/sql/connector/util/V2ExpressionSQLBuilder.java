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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.connector.expressions.Cast;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.types.DataType;

/**
 * The builder to generate SQL from V2 expressions.
 */
public class V2ExpressionSQLBuilder {

  public String build(Expression expr) {
    if (expr instanceof Literal) {
      return visitLiteral((Literal) expr);
    } else if (expr instanceof NamedReference) {
      return visitNamedReference((NamedReference) expr);
    } else if (expr instanceof Cast) {
      Cast cast = (Cast) expr;
      return visitCast(build(cast.expression()), cast.dataType());
    } else if (expr instanceof GeneralScalarExpression) {
      GeneralScalarExpression e = (GeneralScalarExpression) expr;
      String name = e.name();
      switch (name) {
        case "IN": {
          List<String> children =
            Arrays.stream(e.children()).map(c -> build(c)).collect(Collectors.toList());
          return visitIn(children.get(0), children.subList(1, children.size()));
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
        case "LN":
        case "EXP":
        case "POWER":
        case "SQRT":
        case "FLOOR":
        case "CEIL":
        case "WIDTH_BUCKET":
          return visitSQLFunction(name,
            Arrays.stream(e.children()).map(c -> build(c)).toArray(String[]::new));
        case "CASE_WHEN": {
          List<String> children =
            Arrays.stream(e.children()).map(c -> build(c)).collect(Collectors.toList());
          return visitCaseWhen(children.toArray(new String[e.children().length]));
        }
        // TODO supports other expressions
        default:
          return visitUnexpectedExpr(expr);
      }
    } else {
      return visitUnexpectedExpr(expr);
    }
  }

  protected String visitLiteral(Literal literal) {
    return literal.toString();
  }

  protected String visitNamedReference(NamedReference namedRef) {
    return namedRef.toString();
  }

  protected String visitIn(String v, List<String> list) {
    if (list.isEmpty()) {
      return "CASE WHEN " + v + " IS NULL THEN NULL ELSE FALSE END";
    }
    return v + " IN (" + list.stream().collect(Collectors.joining(", ")) + ")";
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
    return l + " LIKE '" + value + "%'";
  }

  protected String visitEndsWith(String l, String r) {
    // Remove quotes at the beginning and end.
    // e.g. converts "'str'" to "str".
    String value = r.substring(1, r.length() - 1);
    return l + " LIKE '%" + value + "'";
  }

  protected String visitContains(String l, String r) {
    // Remove quotes at the beginning and end.
    // e.g. converts "'str'" to "str".
    String value = r.substring(1, r.length() - 1);
    return l + " LIKE '%" + value + "%'";
  }

  private String inputToSQL(Expression input) {
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
    return funcName + "(" + Arrays.stream(inputs).collect(Collectors.joining(", ")) + ")";
  }

  protected String visitUnexpectedExpr(Expression expr) throws IllegalArgumentException {
    throw new IllegalArgumentException("Unexpected V2 expression: " + expr);
  }
}
