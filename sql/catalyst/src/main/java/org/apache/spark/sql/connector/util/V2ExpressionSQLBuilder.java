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

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.errors.QueryExecutionErrors;

/**
 * The builder to generate SQL from V2 expressions.
 */
public class V2ExpressionSQLBuilder {
  public String build(Expression expr) throws Throwable {
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
          case "+":
          case "-":
          case "*":
          case "/":
          case "%":
          case "&&":
          case "||":
          case "AND":
          case "OR":
          case "&":
          case "|":
          case "^":
            return visitBinaryOperation(name, build(e.children()[0]), build(e.children()[1]));
          case "NOT":
            return visitNot(build(e.children()[0]));
          case "CASE_WHEN":
            return visitCaseWhen(e);
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

  protected String visitFieldReference(FieldReference fieldReference) {
    return fieldReference.toString();
  }

  protected String visitIsNull(String v) {
    return v + " IS NULL";
  }

  protected String visitIsNotNull(String v) {
    return v + " IS NOT NULL";
  }

  protected String visitBinaryOperation(String name, String l, String r) {
    return "(" + l + ") " + name + " (" + r + ")";
  }

  protected String visitNot(String v) {
    return "NOT (" + v + ")";
  }

  protected String visitCaseWhen(GeneralScalarExpression e) throws Throwable {
    GeneralScalarExpression branchExpression = (GeneralScalarExpression) e.children()[0];
    StringBuilder sb = new StringBuilder("CASE");
    for (Expression expr: branchExpression.children()) {
      GeneralScalarExpression branch = (GeneralScalarExpression) expr;
      String c = build(branch.children()[0]);
      String v = build(branch.children()[1]);
      sb.append(" WHEN ");
      sb.append(c);
      sb.append(" THEN ");
      sb.append(v);
    }
    if (e.children().length == 2) {
      String elseSQL = build(e.children()[1]);
      sb.append(" ELSE ");
      sb.append(elseSQL);
    }
    sb.append(" END");
    return sb.toString();
  }

  protected String visitUnexpectedExpr(Expression expr) throws Throwable {
    throw QueryExecutionErrors.unexpectedV2ExpressionError(expr);
  }
}
