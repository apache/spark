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

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;
import org.apache.spark.sql.connector.expressions.LiteralValue;

/**
 * The builder to generate SQL from V2 expressions.
 */
public class V2ExpressionSQLBuilder {
  public String build(Expression expr) {
    if (expr instanceof LiteralValue) {
      return visitLiteral((LiteralValue) expr);
    } else if (expr instanceof FieldReference) {
      return visitFieldReference((FieldReference) expr);
    } else if (expr instanceof GeneralScalarExpression) {
      GeneralScalarExpression e = (GeneralScalarExpression) expr;
      String name = e.name();
      switch (name) {
        case "IS_NULL":
          return visitIsNull(build(e.children()[0]));
        case "IS_NOT_NULL":
          return visitIsNotNull(build(e.children()[0]));
        case "=":
        case "!=":
        case "<=>":
        case "<":
        case "<=":
        case ">":
        case ">=":
          return visitBinaryComparison(name, build(e.children()[0]), build(e.children()[1]));
        case "+":
        case "*":
        case "/":
        case "%":
        case "&":
        case "|":
        case "^":
          return visitBinaryArithmetic(name, build(e.children()[0]), build(e.children()[1]));
        case "-":
          if (e.children().length == 1) {
            return visitUnaryArithmetic(name, build(e.children()[0]));
          } else {
            return visitBinaryArithmetic(name, build(e.children()[0]), build(e.children()[1]));
          }
        case "AND":
          return visitAnd(name, build(e.children()[0]), build(e.children()[1]));
        case "OR":
          return visitOr(name, build(e.children()[0]), build(e.children()[1]));
        case "NOT":
          return visitNot(build(e.children()[0]));
        case "~":
          return visitUnaryArithmetic(name, build(e.children()[0]));
        case "CASE_WHEN":
          List<String> children = new ArrayList<>();
          for (Expression child : e.children()) {
            children.add(build(child));
          }
          return visitCaseWhen(children.toArray(new String[e.children().length]));
        // TODO supports other expressions
        default:
          return visitUnexpectedExpr(expr);
      }
    } else {
      return visitUnexpectedExpr(expr);
    }
  }

  protected String visitLiteral(LiteralValue literalValue) {
    return literalValue.toString();
  }

  protected String visitFieldReference(FieldReference fieldRef) {
    return fieldRef.toString();
  }

  protected String visitIsNull(String v) {
    return v + " IS NULL";
  }

  protected String visitIsNotNull(String v) {
    return v + " IS NOT NULL";
  }

  protected String visitBinaryComparison(String name, String l, String r) {
    return "(" + l + ") " + name + " (" + r + ")";
  }

  protected String visitBinaryArithmetic(String name, String l, String r) {
    return "(" + l + ") " + name + " (" + r + ")";
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

  protected String visitUnaryArithmetic(String name, String v) { return name +" (" + v + ")"; }

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

  protected String visitUnexpectedExpr(Expression expr) throws IllegalArgumentException {
    throw new IllegalArgumentException("Unexpected V2 expression: " + expr);
  }
}
