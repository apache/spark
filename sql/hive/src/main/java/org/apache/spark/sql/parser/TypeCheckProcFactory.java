/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.parser;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hive.common.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Factory for creating typecheck processors. The typecheck processors are
 * used to processes the syntax trees for expressions and convert them into
 * expression Node Descriptor trees. They also introduce the correct conversion
 * functions to do proper implicit conversion.
 */
public class TypeCheckProcFactory {

  protected static final Logger LOG = LoggerFactory.getLogger(TypeCheckProcFactory.class
      .getName());

  protected TypeCheckProcFactory() {
    // prevent instantiation
  }

  /**
   * Function to do groupby subexpression elimination. This is called by all the
   * processors initially. As an example, consider the query select a+b,
   * count(1) from T group by a+b; Then a+b is already precomputed in the group
   * by operators key, so we substitute a+b in the select list with the internal
   * column name of the a+b expression that appears in the in input row
   * resolver.
   *
   * @param nd
   *          The node that is being inspected.
   * @param procCtx
   *          The processor context.
   *
   * @return exprNodeColumnDesc.
   */
  public static ExprNodeDesc processGByExpr(Node nd, Object procCtx)
      throws SemanticException {
    // We recursively create the exprNodeDesc. Base cases: when we encounter
    // a column ref, we convert that into an exprNodeColumnDesc; when we
    // encounter
    // a constant, we convert that into an exprNodeConstantDesc. For others we
    // just
    // build the exprNodeFuncDesc with recursively built children.
    ASTNode expr = (ASTNode) nd;
    TypeCheckCtx ctx = (TypeCheckCtx) procCtx;

    if (!ctx.isUseCaching()) {
      return null;
    }

    RowResolver input = ctx.getInputRR();
    ExprNodeDesc desc = null;

    if ((ctx == null) || (input == null) || (!ctx.getAllowGBExprElimination())) {
      return null;
    }

    // If the current subExpression is pre-calculated, as in Group-By etc.
    ColumnInfo colInfo = input.getExpression(expr);
    if (colInfo != null) {
      desc = new ExprNodeColumnDesc(colInfo);
      ASTNode source = input.getExpressionSource(expr);
      if (source != null) {
        ctx.getUnparseTranslator().addCopyTranslation(expr, source);
      }
      return desc;
    }
    return desc;
  }

  public static Map<ASTNode, ExprNodeDesc> genExprNode(ASTNode expr, TypeCheckCtx tcCtx)
      throws SemanticException {
    return genExprNode(expr, tcCtx, new TypeCheckProcFactory());
  }

  protected static Map<ASTNode, ExprNodeDesc> genExprNode(ASTNode expr,
      TypeCheckCtx tcCtx, TypeCheckProcFactory tf) throws SemanticException {
    // Create the walker, the rules dispatcher and the context.
    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    opRules.put(new RuleRegExp("R1", SparkSqlParser.TOK_NULL + "%"),
        tf.getNullExprProcessor());
    opRules.put(new RuleRegExp("R2", SparkSqlParser.Number + "%|" +
        SparkSqlParser.TinyintLiteral + "%|" +
        SparkSqlParser.SmallintLiteral + "%|" +
        SparkSqlParser.BigintLiteral + "%|" +
        SparkSqlParser.DecimalLiteral + "%"),
        tf.getNumExprProcessor());
    opRules
        .put(new RuleRegExp("R3", SparkSqlParser.Identifier + "%|"
        + SparkSqlParser.StringLiteral + "%|" + SparkSqlParser.TOK_CHARSETLITERAL + "%|"
        + SparkSqlParser.TOK_STRINGLITERALSEQUENCE + "%|"
        + "%|" + SparkSqlParser.KW_IF + "%|" + SparkSqlParser.KW_CASE + "%|"
        + SparkSqlParser.KW_WHEN + "%|" + SparkSqlParser.KW_IN + "%|"
        + SparkSqlParser.KW_ARRAY + "%|" + SparkSqlParser.KW_MAP + "%|"
        + SparkSqlParser.KW_STRUCT + "%|" + SparkSqlParser.KW_EXISTS + "%|"
        + SparkSqlParser.TOK_SUBQUERY_OP_NOTIN + "%"),
        tf.getStrExprProcessor());
    opRules.put(new RuleRegExp("R4", SparkSqlParser.KW_TRUE + "%|"
        + SparkSqlParser.KW_FALSE + "%"), tf.getBoolExprProcessor());
    opRules.put(new RuleRegExp("R5", SparkSqlParser.TOK_DATELITERAL + "%|"
        + SparkSqlParser.TOK_TIMESTAMPLITERAL + "%"), tf.getDateTimeExprProcessor());
    opRules.put(new RuleRegExp("R6",
        SparkSqlParser.TOK_INTERVAL_YEAR_MONTH_LITERAL + "%|"
        + SparkSqlParser.TOK_INTERVAL_DAY_TIME_LITERAL + "%|"
        + SparkSqlParser.TOK_INTERVAL_YEAR_LITERAL + "%|"
        + SparkSqlParser.TOK_INTERVAL_MONTH_LITERAL + "%|"
        + SparkSqlParser.TOK_INTERVAL_DAY_LITERAL + "%|"
        + SparkSqlParser.TOK_INTERVAL_HOUR_LITERAL + "%|"
        + SparkSqlParser.TOK_INTERVAL_MINUTE_LITERAL + "%|"
        + SparkSqlParser.TOK_INTERVAL_SECOND_LITERAL + "%"), tf.getIntervalExprProcessor());
    opRules.put(new RuleRegExp("R7", SparkSqlParser.TOK_TABLE_OR_COL + "%"),
        tf.getColumnExprProcessor());
    opRules.put(new RuleRegExp("R8", SparkSqlParser.TOK_SUBQUERY_OP + "%"),
        tf.getSubQueryExprProcessor());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(tf.getDefaultExprProcessor(),
        opRules, tcCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of top nodes
    ArrayList<Node> topNodes = Lists.<Node>newArrayList(expr);
    HashMap<Node, Object> nodeOutputs = new LinkedHashMap<Node, Object>();

    ogw.startWalking(topNodes, nodeOutputs);

    return convert(nodeOutputs);
  }

  // temporary type-safe casting
  private static Map<ASTNode, ExprNodeDesc> convert(Map<Node, Object> outputs) {
    Map<ASTNode, ExprNodeDesc> converted = new LinkedHashMap<ASTNode, ExprNodeDesc>();
    for (Map.Entry<Node, Object> entry : outputs.entrySet()) {
      if (entry.getKey() instanceof ASTNode &&
          (entry.getValue() == null || entry.getValue() instanceof ExprNodeDesc)) {
        converted.put((ASTNode)entry.getKey(), (ExprNodeDesc)entry.getValue());
      } else {
        LOG.warn("Invalid type entry " + entry);
      }
    }
    return converted;
  }

  /**
   * Processor for processing NULL expression.
   */
  public static class NullExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      return new ExprNodeConstantDesc(TypeInfoFactory.getPrimitiveTypeInfoFromPrimitiveWritable(NullWritable.class), null);
    }

  }

  /**
   * Factory method to get NullExprProcessor.
   *
   * @return NullExprProcessor.
   */
  public NullExprProcessor getNullExprProcessor() {
    return new NullExprProcessor();
  }

  /**
   * Processor for processing numeric constants.
   */
  public static class NumExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      Number v = null;
      ASTNode expr = (ASTNode) nd;
      // The expression can be any one of Double, Long and Integer. We
      // try to parse the expression in that order to ensure that the
      // most specific type is used for conversion.
      try {
        if (expr.getText().endsWith("L")) {
          // Literal bigint.
          v = Long.valueOf(expr.getText().substring(
                0, expr.getText().length() - 1));
        } else if (expr.getText().endsWith("S")) {
          // Literal smallint.
          v = Short.valueOf(expr.getText().substring(
                0, expr.getText().length() - 1));
        } else if (expr.getText().endsWith("Y")) {
          // Literal tinyint.
          v = Byte.valueOf(expr.getText().substring(
                0, expr.getText().length() - 1));
        } else if (expr.getText().endsWith("BD")) {
          // Literal decimal
          String strVal = expr.getText().substring(0, expr.getText().length() - 2);
          HiveDecimal hd = HiveDecimal.create(strVal);
          int prec = 1;
          int scale = 0;
          if (hd != null) {
            prec = hd.precision();
            scale = hd.scale();
          }
          DecimalTypeInfo typeInfo = TypeInfoFactory.getDecimalTypeInfo(prec, scale);
          return new ExprNodeConstantDesc(typeInfo, hd);
        } else {
          v = Double.valueOf(expr.getText());
          v = Long.valueOf(expr.getText());
          v = Integer.valueOf(expr.getText());
        }
      } catch (NumberFormatException e) {
        // do nothing here, we will throw an exception in the following block
      }
      if (v == null) {
        throw new SemanticException(ErrorMsg.INVALID_NUMERICAL_CONSTANT
            .getMsg(expr));
      }
      return new ExprNodeConstantDesc(v);
    }

  }

  /**
   * Factory method to get NumExprProcessor.
   *
   * @return NumExprProcessor.
   */
  public NumExprProcessor getNumExprProcessor() {
    return new NumExprProcessor();
  }

  /**
   * Processor for processing string constants.
   */
  public static class StrExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      String str = null;

      switch (expr.getToken().getType()) {
      case SparkSqlParser.StringLiteral:
        str = SemanticAnalyzer.unescapeSQLString(expr.getText());
        break;
      case SparkSqlParser.TOK_STRINGLITERALSEQUENCE:
        StringBuilder sb = new StringBuilder();
        for (Node n : expr.getChildren()) {
          sb.append(
              SemanticAnalyzer.unescapeSQLString(((ASTNode)n).getText()));
        }
        str = sb.toString();
        break;
      case SparkSqlParser.TOK_CHARSETLITERAL:
        str = SemanticAnalyzer.charSetString(expr.getChild(0).getText(),
            expr.getChild(1).getText());
        break;
      default:
        // SparkSqlParser.identifier | HiveParse.KW_IF | HiveParse.KW_LEFT |
        // HiveParse.KW_RIGHT
        str = SemanticAnalyzer.unescapeIdentifier(expr.getText());
        break;
      }
      return new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, str);
    }

  }

  /**
   * Factory method to get StrExprProcessor.
   *
   * @return StrExprProcessor.
   */
  public StrExprProcessor getStrExprProcessor() {
    return new StrExprProcessor();
  }

  /**
   * Processor for boolean constants.
   */
  public static class BoolExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      Boolean bool = null;

      switch (expr.getToken().getType()) {
      case SparkSqlParser.KW_TRUE:
        bool = Boolean.TRUE;
        break;
      case SparkSqlParser.KW_FALSE:
        bool = Boolean.FALSE;
        break;
      default:
        assert false;
      }
      return new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, bool);
    }

  }

  /**
   * Factory method to get BoolExprProcessor.
   *
   * @return BoolExprProcessor.
   */
  public BoolExprProcessor getBoolExprProcessor() {
    return new BoolExprProcessor();
  }

  /**
   * Processor for date constants.
   */
  public static class DateTimeExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      String timeString = SemanticAnalyzer.stripQuotes(expr.getText());

      // Get the string value and convert to a Date value.
      try {
        // todo replace below with joda-time, which supports timezone
        if (expr.getType() == SparkSqlParser.TOK_DATELITERAL) {
          PrimitiveTypeInfo typeInfo = TypeInfoFactory.dateTypeInfo;
          return new ExprNodeConstantDesc(typeInfo,
              Date.valueOf(timeString));
        }
        if (expr.getType() == SparkSqlParser.TOK_TIMESTAMPLITERAL) {
          return new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo,
              Timestamp.valueOf(timeString));
        }
        throw new IllegalArgumentException("Invalid time literal type " + expr.getType());
      } catch (Exception err) {
        throw new SemanticException(
            "Unable to convert time literal '" + timeString + "' to time value.", err);
      }
    }
  }

  /**
   * Factory method to get DateExprProcessor.
   *
   * @return DateExprProcessor.
   */
  public DateTimeExprProcessor getDateTimeExprProcessor() {
    return new DateTimeExprProcessor();
  }

  /**
   * Processor for interval constants.
   */
  public static class IntervalExprProcessor implements NodeProcessor {

    private static final BigDecimal NANOS_PER_SEC_BD = new BigDecimal(DateUtils.NANOS_PER_SEC);
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      String intervalString = SemanticAnalyzer.stripQuotes(expr.getText());

      // Get the string value and convert to a Interval value.
      try {
        switch (expr.getType()) {
          case SparkSqlParser.TOK_INTERVAL_YEAR_MONTH_LITERAL:
            return new ExprNodeConstantDesc(TypeInfoFactory.intervalYearMonthTypeInfo,
                HiveIntervalYearMonth.valueOf(intervalString));
          case SparkSqlParser.TOK_INTERVAL_DAY_TIME_LITERAL:
            return new ExprNodeConstantDesc(TypeInfoFactory.intervalDayTimeTypeInfo,
                HiveIntervalDayTime.valueOf(intervalString));
          case SparkSqlParser.TOK_INTERVAL_YEAR_LITERAL:
            return new ExprNodeConstantDesc(TypeInfoFactory.intervalYearMonthTypeInfo,
                new HiveIntervalYearMonth(Integer.parseInt(intervalString), 0));
          case SparkSqlParser.TOK_INTERVAL_MONTH_LITERAL:
            return new ExprNodeConstantDesc(TypeInfoFactory.intervalYearMonthTypeInfo,
                new HiveIntervalYearMonth(0, Integer.parseInt(intervalString)));
          case SparkSqlParser.TOK_INTERVAL_DAY_LITERAL:
            return new ExprNodeConstantDesc(TypeInfoFactory.intervalDayTimeTypeInfo,
                new HiveIntervalDayTime(Integer.parseInt(intervalString), 0, 0, 0, 0));
          case SparkSqlParser.TOK_INTERVAL_HOUR_LITERAL:
            return new ExprNodeConstantDesc(TypeInfoFactory.intervalDayTimeTypeInfo,
                new HiveIntervalDayTime(0, Integer.parseInt(intervalString), 0, 0, 0));
          case SparkSqlParser.TOK_INTERVAL_MINUTE_LITERAL:
            return new ExprNodeConstantDesc(TypeInfoFactory.intervalDayTimeTypeInfo,
                new HiveIntervalDayTime(0, 0, Integer.parseInt(intervalString), 0, 0));
          case SparkSqlParser.TOK_INTERVAL_SECOND_LITERAL:
            BigDecimal bd = new BigDecimal(intervalString);
            BigDecimal bdSeconds = new BigDecimal(bd.toBigInteger());
            BigDecimal bdNanos = bd.subtract(bdSeconds);
            return new ExprNodeConstantDesc(TypeInfoFactory.intervalDayTimeTypeInfo,
                new HiveIntervalDayTime(0, 0, 0, bdSeconds.intValueExact(),
                    bdNanos.multiply(NANOS_PER_SEC_BD).intValue()));
          default:
            throw new IllegalArgumentException("Invalid time literal type " + expr.getType());
        }
      } catch (Exception err) {
        throw new SemanticException(
            "Unable to convert interval literal '" + intervalString + "' to interval value.", err);
      }
    }
  }

  /**
   * Factory method to get IntervalExprProcessor.
   *
   * @return IntervalExprProcessor.
   */
  public IntervalExprProcessor getIntervalExprProcessor() {
    return new IntervalExprProcessor();
  }

  /**
   * Processor for table columns.
   */
  public static class ColumnExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      ASTNode parent = stack.size() > 1 ? (ASTNode) stack.get(stack.size() - 2) : null;
      RowResolver input = ctx.getInputRR();

      if (expr.getType() != SparkSqlParser.TOK_TABLE_OR_COL) {
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr), expr);
        return null;
      }

      assert (expr.getChildCount() == 1);
      String tableOrCol = SemanticAnalyzer.unescapeIdentifier(expr
          .getChild(0).getText());

      boolean isTableAlias = input.hasTableAlias(tableOrCol);
      ColumnInfo colInfo = input.get(null, tableOrCol);

      if (isTableAlias) {
        if (colInfo != null) {
          if (parent != null && parent.getType() == SparkSqlParser.DOT) {
            // It's a table alias.
            return null;
          }
          // It's a column.
          return toExprNodeDesc(colInfo);
        } else {
          // It's a table alias.
          // We will process that later in DOT.
          return null;
        }
      } else {
        if (colInfo == null) {
          // It's not a column or a table alias.
          if (input.getIsExprResolver()) {
            ASTNode exprNode = expr;
            if (!stack.empty()) {
              ASTNode tmp = (ASTNode) stack.pop();
              if (!stack.empty()) {
                exprNode = (ASTNode) stack.peek();
              }
              stack.push(tmp);
            }
            ctx.setError(ErrorMsg.NON_KEY_EXPR_IN_GROUPBY.getMsg(exprNode), expr);
            return null;
          } else {
            List<String> possibleColumnNames = input.getReferenceableColumnAliases(tableOrCol, -1);
            String reason = String.format("(possible column names are: %s)",
                StringUtils.join(possibleColumnNames, ", "));
            ctx.setError(ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr.getChild(0), reason),
                expr);
            LOG.debug(ErrorMsg.INVALID_TABLE_OR_COLUMN.toString() + ":"
                + input.toString());
            return null;
          }
        } else {
          // It's a column.
          return toExprNodeDesc(colInfo);
        }
      }

    }

  }

  private static ExprNodeDesc toExprNodeDesc(ColumnInfo colInfo) {
    ObjectInspector inspector = colInfo.getObjectInspector();
    if (inspector instanceof ConstantObjectInspector &&
        inspector instanceof PrimitiveObjectInspector) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) inspector;
      Object constant = ((ConstantObjectInspector) inspector).getWritableConstantValue();
      return new ExprNodeConstantDesc(colInfo.getType(), poi.getPrimitiveJavaObject(constant));
    }
    // non-constant or non-primitive constants
    ExprNodeColumnDesc column = new ExprNodeColumnDesc(colInfo);
    column.setSkewedCol(colInfo.isSkewedCol());
    return column;
  }

  /**
   * Factory method to get ColumnExprProcessor.
   *
   * @return ColumnExprProcessor.
   */
  public ColumnExprProcessor getColumnExprProcessor() {
    return new ColumnExprProcessor();
  }

  /**
   * The default processor for typechecking.
   */
  public static class DefaultExprProcessor implements NodeProcessor {

    static HashMap<Integer, String> specialUnaryOperatorTextHashMap;
    static HashMap<Integer, String> specialFunctionTextHashMap;
    static HashMap<Integer, String> conversionFunctionTextHashMap;
    static HashSet<Integer> windowingTokens;
    static {
      specialUnaryOperatorTextHashMap = new HashMap<Integer, String>();
      specialUnaryOperatorTextHashMap.put(SparkSqlParser.PLUS, "positive");
      specialUnaryOperatorTextHashMap.put(SparkSqlParser.MINUS, "negative");
      specialFunctionTextHashMap = new HashMap<Integer, String>();
      specialFunctionTextHashMap.put(SparkSqlParser.TOK_ISNULL, "isnull");
      specialFunctionTextHashMap.put(SparkSqlParser.TOK_ISNOTNULL, "isnotnull");
      conversionFunctionTextHashMap = new HashMap<Integer, String>();
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_BOOLEAN,
          serdeConstants.BOOLEAN_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_TINYINT,
          serdeConstants.TINYINT_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_SMALLINT,
          serdeConstants.SMALLINT_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_INT,
          serdeConstants.INT_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_BIGINT,
          serdeConstants.BIGINT_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_FLOAT,
          serdeConstants.FLOAT_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_DOUBLE,
          serdeConstants.DOUBLE_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_STRING,
          serdeConstants.STRING_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_CHAR,
          serdeConstants.CHAR_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_VARCHAR,
          serdeConstants.VARCHAR_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_BINARY,
          serdeConstants.BINARY_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_DATE,
          serdeConstants.DATE_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_TIMESTAMP,
          serdeConstants.TIMESTAMP_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_INTERVAL_YEAR_MONTH,
          serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_INTERVAL_DAY_TIME,
          serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
      conversionFunctionTextHashMap.put(SparkSqlParser.TOK_DECIMAL,
          serdeConstants.DECIMAL_TYPE_NAME);

      windowingTokens = new HashSet<Integer>();
      windowingTokens.add(SparkSqlParser.KW_OVER);
      windowingTokens.add(SparkSqlParser.TOK_PARTITIONINGSPEC);
      windowingTokens.add(SparkSqlParser.TOK_DISTRIBUTEBY);
      windowingTokens.add(SparkSqlParser.TOK_SORTBY);
      windowingTokens.add(SparkSqlParser.TOK_CLUSTERBY);
      windowingTokens.add(SparkSqlParser.TOK_WINDOWSPEC);
      windowingTokens.add(SparkSqlParser.TOK_WINDOWRANGE);
      windowingTokens.add(SparkSqlParser.TOK_WINDOWVALUES);
      windowingTokens.add(SparkSqlParser.KW_UNBOUNDED);
      windowingTokens.add(SparkSqlParser.KW_PRECEDING);
      windowingTokens.add(SparkSqlParser.KW_FOLLOWING);
      windowingTokens.add(SparkSqlParser.KW_CURRENT);
      windowingTokens.add(SparkSqlParser.TOK_TABSORTCOLNAMEASC);
      windowingTokens.add(SparkSqlParser.TOK_TABSORTCOLNAMEDESC);
    }

    protected static boolean isRedundantConversionFunction(ASTNode expr,
        boolean isFunction, ArrayList<ExprNodeDesc> children) {
      if (!isFunction) {
        return false;
      }
      // conversion functions take a single parameter
      if (children.size() != 1) {
        return false;
      }
      String funcText = conversionFunctionTextHashMap.get(((ASTNode) expr
          .getChild(0)).getType());
      // not a conversion function
      if (funcText == null) {
        return false;
      }
      // return true when the child type and the conversion target type is the
      // same
      return ((PrimitiveTypeInfo) children.get(0).getTypeInfo()).getTypeName()
          .equalsIgnoreCase(funcText);
    }

    public static String getFunctionText(ASTNode expr, boolean isFunction) {
      String funcText = null;
      if (!isFunction) {
        // For operator, the function name is the operator text, unless it's in
        // our special dictionary
        if (expr.getChildCount() == 1) {
          funcText = specialUnaryOperatorTextHashMap.get(expr.getType());
        }
        if (funcText == null) {
          funcText = expr.getText();
        }
      } else {
        // For TOK_FUNCTION, the function name is stored in the first child,
        // unless it's in our
        // special dictionary.
        assert (expr.getChildCount() >= 1);
        int funcType = ((ASTNode) expr.getChild(0)).getType();
        funcText = specialFunctionTextHashMap.get(funcType);
        if (funcText == null) {
          funcText = conversionFunctionTextHashMap.get(funcType);
        }
        if (funcText == null) {
          funcText = ((ASTNode) expr.getChild(0)).getText();
        }
      }
      return SemanticAnalyzer.unescapeIdentifier(funcText);
    }

    /**
     * This function create an ExprNodeDesc for a UDF function given the
     * children (arguments). It will insert implicit type conversion functions
     * if necessary.
     *
     * @throws UDFArgumentException
     */
    static ExprNodeDesc getFuncExprNodeDescWithUdfData(String udfName, TypeInfo typeInfo,
        ExprNodeDesc... children) throws UDFArgumentException {

      FunctionInfo fi;
      try {
        fi = FunctionRegistry.getFunctionInfo(udfName);
      } catch (SemanticException e) {
        throw new UDFArgumentException(e);
      }
      if (fi == null) {
        throw new UDFArgumentException(udfName + " not found.");
      }

      GenericUDF genericUDF = fi.getGenericUDF();
      if (genericUDF == null) {
        throw new UDFArgumentException(udfName
            + " is an aggregation function or a table function.");
      }

      // Add udfData to UDF if necessary
      if (typeInfo != null) {
        if (genericUDF instanceof SettableUDF) {
          ((SettableUDF)genericUDF).setTypeInfo(typeInfo);
        }
      }
      
      List<ExprNodeDesc> childrenList = new ArrayList<ExprNodeDesc>(children.length);

      childrenList.addAll(Arrays.asList(children));
      return ExprNodeGenericFuncDesc.newInstance(genericUDF,
          childrenList);
    }

    public static ExprNodeDesc getFuncExprNodeDesc(String udfName,
        ExprNodeDesc... children) throws UDFArgumentException {
      return getFuncExprNodeDescWithUdfData(udfName, null, children);
    }

    protected void validateUDF(ASTNode expr, boolean isFunction, TypeCheckCtx ctx, FunctionInfo fi,
        List<ExprNodeDesc> children, GenericUDF genericUDF) throws SemanticException {
      // Detect UDTF's in nested SELECT, GROUP BY, etc as they aren't
      // supported
      if (fi.getGenericUDTF() != null) {
        throw new SemanticException(ErrorMsg.UDTF_INVALID_LOCATION.getMsg());
      }
      // UDAF in filter condition, group-by caluse, param of funtion, etc.
      if (fi.getGenericUDAFResolver() != null) {
        if (isFunction) {
          throw new SemanticException(ErrorMsg.UDAF_INVALID_LOCATION.getMsg((ASTNode) expr
              .getChild(0)));
        } else {
          throw new SemanticException(ErrorMsg.UDAF_INVALID_LOCATION.getMsg(expr));
        }
      }
      if (!ctx.getAllowStatefulFunctions() && (genericUDF != null)) {
        if (FunctionRegistry.isStateful(genericUDF)) {
          throw new SemanticException(ErrorMsg.UDF_STATEFUL_INVALID_LOCATION.getMsg());
        }
      }
    }

    protected ExprNodeDesc getXpathOrFuncExprNodeDesc(ASTNode expr,
        boolean isFunction, ArrayList<ExprNodeDesc> children, TypeCheckCtx ctx)
        throws SemanticException, UDFArgumentException {
      // return the child directly if the conversion is redundant.
      if (isRedundantConversionFunction(expr, isFunction, children)) {
        assert (children.size() == 1);
        assert (children.get(0) != null);
        return children.get(0);
      }
      String funcText = getFunctionText(expr, isFunction);
      ExprNodeDesc desc;
      if (funcText.equals(".")) {
        // "." : FIELD Expression

        assert (children.size() == 2);
        // Only allow constant field name for now
        assert (children.get(1) instanceof ExprNodeConstantDesc);
        ExprNodeDesc object = children.get(0);
        ExprNodeConstantDesc fieldName = (ExprNodeConstantDesc) children.get(1);
        assert (fieldName.getValue() instanceof String);

        // Calculate result TypeInfo
        String fieldNameString = (String) fieldName.getValue();
        TypeInfo objectTypeInfo = object.getTypeInfo();

        // Allow accessing a field of list element structs directly from a list
        boolean isList = (object.getTypeInfo().getCategory() == ObjectInspector.Category.LIST);
        if (isList) {
          objectTypeInfo = ((ListTypeInfo) objectTypeInfo).getListElementTypeInfo();
        }
        if (objectTypeInfo.getCategory() != Category.STRUCT) {
          throw new SemanticException(ErrorMsg.INVALID_DOT.getMsg(expr));
        }
        TypeInfo t = ((StructTypeInfo) objectTypeInfo).getStructFieldTypeInfo(fieldNameString);
        if (isList) {
          t = TypeInfoFactory.getListTypeInfo(t);
        }

        desc = new ExprNodeFieldDesc(t, children.get(0), fieldNameString, isList);
      } else if (funcText.equals("[")) {
        // "[]" : LSQUARE/INDEX Expression
        if (!ctx.getallowIndexExpr()) {
          throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(expr));
        }

        assert (children.size() == 2);

        // Check whether this is a list or a map
        TypeInfo myt = children.get(0).getTypeInfo();

        if (myt.getCategory() == Category.LIST) {
          // Only allow integer index for now
          if (!(children.get(1) instanceof ExprNodeConstantDesc)
              || !(((ExprNodeConstantDesc) children.get(1)).getTypeInfo()
              .equals(TypeInfoFactory.intTypeInfo))) {
            throw new SemanticException(SemanticAnalyzer.generateErrorMessage(
                expr,
                ErrorMsg.INVALID_ARRAYINDEX_TYPE.getMsg()));
          }

          // Calculate TypeInfo
          TypeInfo t = ((ListTypeInfo) myt).getListElementTypeInfo();
          desc = new ExprNodeGenericFuncDesc(t, FunctionRegistry.getGenericUDFForIndex(), children);
        } else if (myt.getCategory() == Category.MAP) {
          if (!(children.get(1) instanceof ExprNodeConstantDesc)) {
            throw new SemanticException(SemanticAnalyzer.generateErrorMessage(
                expr,
                ErrorMsg.INVALID_MAPINDEX_CONSTANT.getMsg()));
          }
          // Calculate TypeInfo
          TypeInfo t = ((MapTypeInfo) myt).getMapValueTypeInfo();
          desc = new ExprNodeGenericFuncDesc(t, FunctionRegistry.getGenericUDFForIndex(), children);
        } else {
          throw new SemanticException(ErrorMsg.NON_COLLECTION_TYPE.getMsg(expr, myt.getTypeName()));
        }
      } else {
        // other operators or functions
        FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcText);

        if (fi == null) {
          if (isFunction) {
            throw new SemanticException(ErrorMsg.INVALID_FUNCTION
                .getMsg((ASTNode) expr.getChild(0)));
          } else {
            throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(expr));
          }
        }

        // getGenericUDF() actually clones the UDF. Just call it once and reuse.
        GenericUDF genericUDF = fi.getGenericUDF();

        if (!fi.isNative()) {
          ctx.getUnparseTranslator().addIdentifierTranslation(
              (ASTNode) expr.getChild(0));
        }

        // Handle type casts that may contain type parameters
        if (isFunction) {
          ASTNode funcNameNode = (ASTNode)expr.getChild(0);
          switch (funcNameNode.getType()) {
            case SparkSqlParser.TOK_CHAR:
              // Add type params
              CharTypeInfo charTypeInfo = ParseUtils.getCharTypeInfo(funcNameNode);
              if (genericUDF != null) {
                ((SettableUDF)genericUDF).setTypeInfo(charTypeInfo);
              }
              break;
            case SparkSqlParser.TOK_VARCHAR:
              VarcharTypeInfo varcharTypeInfo = ParseUtils.getVarcharTypeInfo(funcNameNode);
              if (genericUDF != null) {
                ((SettableUDF)genericUDF).setTypeInfo(varcharTypeInfo);
              }
              break;
            case SparkSqlParser.TOK_DECIMAL:
              DecimalTypeInfo decTypeInfo = ParseUtils.getDecimalTypeTypeInfo(funcNameNode);
              if (genericUDF != null) {
                ((SettableUDF)genericUDF).setTypeInfo(decTypeInfo);
              }
              break;
            default:
              // Do nothing
              break;
          }
        }

        validateUDF(expr, isFunction, ctx, fi, children, genericUDF);

        // Try to infer the type of the constant only if there are two
        // nodes, one of them is column and the other is numeric const
        if (genericUDF instanceof GenericUDFBaseCompare
            && children.size() == 2
            && ((children.get(0) instanceof ExprNodeConstantDesc
                && children.get(1) instanceof ExprNodeColumnDesc)
                || (children.get(0) instanceof ExprNodeColumnDesc
                    && children.get(1) instanceof ExprNodeConstantDesc))) {
          int constIdx =
              children.get(0) instanceof ExprNodeConstantDesc ? 0 : 1;

          Set<String> inferTypes = new HashSet<String>(Arrays.asList(
              serdeConstants.TINYINT_TYPE_NAME.toLowerCase(),
              serdeConstants.SMALLINT_TYPE_NAME.toLowerCase(),
              serdeConstants.INT_TYPE_NAME.toLowerCase(),
              serdeConstants.BIGINT_TYPE_NAME.toLowerCase(),
              serdeConstants.FLOAT_TYPE_NAME.toLowerCase(),
              serdeConstants.DOUBLE_TYPE_NAME.toLowerCase(),
              serdeConstants.STRING_TYPE_NAME.toLowerCase()
              ));

          String constType = children.get(constIdx).getTypeString().toLowerCase();
          String columnType = children.get(1 - constIdx).getTypeString().toLowerCase();

          if (inferTypes.contains(constType) && inferTypes.contains(columnType)
              && !columnType.equalsIgnoreCase(constType)) {
            Object originalValue =  ((ExprNodeConstantDesc) children.get(constIdx)).getValue();
            String constValue = originalValue.toString();
            boolean triedDouble = false;
            Number value = null;
            try {
              if (columnType.equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME)) {
                value = new Byte(constValue);
              } else if (columnType.equalsIgnoreCase(serdeConstants.SMALLINT_TYPE_NAME)) {
                value = new Short(constValue);
              } else if (columnType.equalsIgnoreCase(serdeConstants.INT_TYPE_NAME)) {
                value = new Integer(constValue);
              } else if (columnType.equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME)) {
                value = new Long(constValue);
              } else if (columnType.equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME)) {
                value = new Float(constValue);
              } else if (columnType.equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME)) {
                triedDouble = true;
                value = new Double(constValue);
              } else if (columnType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)) {
                // Don't scramble the const type information if comparing to a string column,
                // It's not useful to do so; as of now, there is also a hack in
                // SemanticAnalyzer#genTablePlan that causes every column to look like a string
                // a string down here, so number type information is always lost otherwise.
                boolean isNumber = (originalValue instanceof Number);
                triedDouble = !isNumber;
                value = isNumber ? (Number)originalValue : new Double(constValue);
              }
            } catch (NumberFormatException nfe) {
              // this exception suggests the precise type inference did not succeed
              // we'll try again to convert it to double
              // however, if we already tried this, or the column is NUMBER type and
              // the operator is EQUAL, return false due to the type mismatch
              if (triedDouble &&
                  (genericUDF instanceof GenericUDFOPEqual
                  && !columnType.equals(serdeConstants.STRING_TYPE_NAME))) {
                return new ExprNodeConstantDesc(false);
              }

              try {
                value = new Double(constValue);
              } catch (NumberFormatException ex) {
                return new ExprNodeConstantDesc(false);
              }
            }

            if (value != null) {
              children.set(constIdx, new ExprNodeConstantDesc(value));
            }
          }

          // if column type is char and constant type is string, then convert the constant to char
          // type with padded spaces.
          final PrimitiveTypeInfo colTypeInfo = TypeInfoFactory
              .getPrimitiveTypeInfo(columnType);
          if (constType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME) &&
              colTypeInfo instanceof CharTypeInfo) {
            final Object originalValue = ((ExprNodeConstantDesc) children.get(constIdx)).getValue();
            final String constValue = originalValue.toString();
            final int length = TypeInfoUtils.getCharacterLengthForType(colTypeInfo);
            final HiveChar newValue = new HiveChar(constValue, length);
            children.set(constIdx, new ExprNodeConstantDesc(colTypeInfo, newValue));
          }
        }
        if (genericUDF instanceof GenericUDFOPOr) {
          // flatten OR
          List<ExprNodeDesc> childrenList = new ArrayList<ExprNodeDesc>(
              children.size());
          for (ExprNodeDesc child : children) {
            if (FunctionRegistry.isOpOr(child)) {
              childrenList.addAll(child.getChildren());
            } else {
              childrenList.add(child);
            }
          }
          desc = ExprNodeGenericFuncDesc.newInstance(genericUDF, funcText,
              childrenList);
        } else if (genericUDF instanceof GenericUDFOPAnd) {
          // flatten AND
          List<ExprNodeDesc> childrenList = new ArrayList<ExprNodeDesc>(
              children.size());
          for (ExprNodeDesc child : children) {
            if (FunctionRegistry.isOpAnd(child)) {
              childrenList.addAll(child.getChildren());
            } else {
              childrenList.add(child);
            }
          }
          desc = ExprNodeGenericFuncDesc.newInstance(genericUDF, funcText,
              childrenList);
        } else {
          desc = ExprNodeGenericFuncDesc.newInstance(genericUDF, funcText,
              children);
        }
      }
      // UDFOPPositive is a no-op.
      // However, we still create it, and then remove it here, to make sure we
      // only allow
      // "+" for numeric types.
      if (FunctionRegistry.isOpPositive(desc)) {
        assert (desc.getChildren().size() == 1);
        desc = desc.getChildren().get(0);
      }
      assert (desc != null);
      return desc;
    }

    /**
     * Returns true if des is a descendant of ans (ancestor)
     */
    private boolean isDescendant(Node ans, Node des) {
      if (ans.getChildren() == null) {
        return false;
      }
      for (Node c : ans.getChildren()) {
        if (c == des) {
          return true;
        }
        if (isDescendant(c, des)) {
          return true;
        }
      }
      return false;
    }

    protected ExprNodeDesc processQualifiedColRef(TypeCheckCtx ctx, ASTNode expr,
        Object... nodeOutputs) throws SemanticException {
      RowResolver input = ctx.getInputRR();
      String tableAlias = SemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getChild(0)
          .getText());
      // NOTE: tableAlias must be a valid non-ambiguous table alias,
      // because we've checked that in TOK_TABLE_OR_COL's process method.
      String colName;
      if (nodeOutputs[1] instanceof ExprNodeConstantDesc) {
        colName = ((ExprNodeConstantDesc) nodeOutputs[1]).getValue().toString();
      }  else if (nodeOutputs[1] instanceof ExprNodeColumnDesc) {
        colName = ((ExprNodeColumnDesc)nodeOutputs[1]).getColumn();
      } else {
        throw new SemanticException("Unexpected ExprNode : " + nodeOutputs[1]);
      }
      ColumnInfo colInfo = input.get(tableAlias, colName);

      if (colInfo == null) {
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr.getChild(1)), expr);
        return null;
      }
      return toExprNodeDesc(colInfo);
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        // Here we know nd represents a group by expression.

        // During the DFS traversal of the AST, a descendant of nd likely set an
        // error because a sub-tree of nd is unlikely to also be a group by
        // expression. For example, in a query such as
        // SELECT *concat(key)* FROM src GROUP BY concat(key), 'key' will be
        // processed before 'concat(key)' and since 'key' is not a group by
        // expression, an error will be set in ctx by ColumnExprProcessor.

        // We can clear the global error when we see that it was set in a
        // descendant node of a group by expression because
        // processGByExpr() returns a ExprNodeDesc that effectively ignores
        // its children. Although the error can be set multiple times by
        // descendant nodes, DFS traversal ensures that the error only needs to
        // be cleared once. Also, for a case like
        // SELECT concat(value, concat(value))... the logic still works as the
        // error is only set with the first 'value'; all node processors quit
        // early if the global error is set.

        if (isDescendant(nd, ctx.getErrorSrcNode())) {
          ctx.setError(null, null);
        }
        return desc;
      }

      if (ctx.getError() != null) {
        return null;
      }

      ASTNode expr = (ASTNode) nd;

      /*
       * A Windowing specification get added as a child to a UDAF invocation to distinguish it
       * from similar UDAFs but on different windows.
       * The UDAF is translated to a WindowFunction invocation in the PTFTranslator.
       * So here we just return null for tokens that appear in a Window Specification.
       * When the traversal reaches up to the UDAF invocation its ExprNodeDesc is build using the
       * ColumnInfo in the InputRR. This is similar to how UDAFs are handled in Select lists.
       * The difference is that there is translation for Window related tokens, so we just
       * return null;
       */
      if (windowingTokens.contains(expr.getType())) {
        if (!ctx.getallowWindowing()) {
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(expr,
              ErrorMsg.INVALID_FUNCTION.getMsg("Windowing is not supported in the context")));
        }
        return null;
      }

      if (expr.getType() == SparkSqlParser.TOK_TABNAME) {
        return null;
      }

      if (expr.getType() == SparkSqlParser.TOK_ALLCOLREF) {
        if (!ctx.getallowAllColRef()) {
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(expr,
              ErrorMsg.INVALID_COLUMN
                  .getMsg("All column reference is not supported in the context")));
        }

        RowResolver input = ctx.getInputRR();
        ExprNodeColumnListDesc columnList = new ExprNodeColumnListDesc();
        assert expr.getChildCount() <= 1;
        if (expr.getChildCount() == 1) {
          // table aliased (select a.*, for example)
          ASTNode child = (ASTNode) expr.getChild(0);
          assert child.getType() == SparkSqlParser.TOK_TABNAME;
          assert child.getChildCount() == 1;
          String tableAlias = SemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText());
          HashMap<String, ColumnInfo> columns = input.getFieldMap(tableAlias);
          if (columns == null) {
            throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(child));
          }
          for (Map.Entry<String, ColumnInfo> colMap : columns.entrySet()) {
            ColumnInfo colInfo = colMap.getValue();
            if (!colInfo.getIsVirtualCol()) {
              columnList.addColumn(toExprNodeDesc(colInfo));
            }
          }
        } else {
          // all columns (select *, for example)
          for (ColumnInfo colInfo : input.getColumnInfos()) {
            if (!colInfo.getIsVirtualCol()) {
              columnList.addColumn(toExprNodeDesc(colInfo));
            }
          }
        }
        return columnList;
      }

      // If the first child is a TOK_TABLE_OR_COL, and nodeOutput[0] is NULL,
      // and the operator is a DOT, then it's a table column reference.
      if (expr.getType() == SparkSqlParser.DOT
          && expr.getChild(0).getType() == SparkSqlParser.TOK_TABLE_OR_COL
          && nodeOutputs[0] == null) {
        return processQualifiedColRef(ctx, expr, nodeOutputs);
      }

      // Return nulls for conversion operators
      if (conversionFunctionTextHashMap.keySet().contains(expr.getType())
          || specialFunctionTextHashMap.keySet().contains(expr.getType())
          || expr.getToken().getType() == SparkSqlParser.CharSetName
          || expr.getToken().getType() == SparkSqlParser.CharSetLiteral) {
        return null;
      }

      boolean isFunction = (expr.getType() == SparkSqlParser.TOK_FUNCTION ||
          expr.getType() == SparkSqlParser.TOK_FUNCTIONSTAR ||
          expr.getType() == SparkSqlParser.TOK_FUNCTIONDI);

      if (!ctx.getAllowDistinctFunctions() && expr.getType() == SparkSqlParser.TOK_FUNCTIONDI) {
        throw new SemanticException(
            SemanticAnalyzer.generateErrorMessage(expr, ErrorMsg.DISTINCT_NOT_SUPPORTED.getMsg()));
      }

      // Create all children
      int childrenBegin = (isFunction ? 1 : 0);
      ArrayList<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(
          expr.getChildCount() - childrenBegin);
      for (int ci = childrenBegin; ci < expr.getChildCount(); ci++) {
        if (nodeOutputs[ci] instanceof ExprNodeColumnListDesc) {
          children.addAll(((ExprNodeColumnListDesc) nodeOutputs[ci]).getChildren());
        } else {
          children.add((ExprNodeDesc) nodeOutputs[ci]);
        }
      }

      if (expr.getType() == SparkSqlParser.TOK_FUNCTIONSTAR) {
        if (!ctx.getallowFunctionStar()) {
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(expr,
              ErrorMsg.INVALID_COLUMN
                  .getMsg(".* reference is not supported in the context")));
        }

        RowResolver input = ctx.getInputRR();
        for (ColumnInfo colInfo : input.getColumnInfos()) {
          if (!colInfo.getIsVirtualCol()) {
            children.add(toExprNodeDesc(colInfo));
          }
        }
      }

      // If any of the children contains null, then return a null
      // this is a hack for now to handle the group by case
      if (children.contains(null)) {
        List<String> possibleColumnNames = getReferenceableColumnAliases(ctx);
        String reason = String.format("(possible column names are: %s)",
            StringUtils.join(possibleColumnNames, ", "));
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr.getChild(0), reason),
            expr);
        return null;
      }

      // Create function desc
      try {
        return getXpathOrFuncExprNodeDesc(expr, isFunction, children, ctx);
      } catch (UDFArgumentTypeException e) {
        throw new SemanticException(ErrorMsg.INVALID_ARGUMENT_TYPE.getMsg(expr
            .getChild(childrenBegin + e.getArgumentId()), e.getMessage()));
      } catch (UDFArgumentLengthException e) {
        throw new SemanticException(ErrorMsg.INVALID_ARGUMENT_LENGTH.getMsg(
            expr, e.getMessage()));
      } catch (UDFArgumentException e) {
        throw new SemanticException(ErrorMsg.INVALID_ARGUMENT.getMsg(expr, e
            .getMessage()));
      }
    }

    protected List<String> getReferenceableColumnAliases(TypeCheckCtx ctx) {
      return ctx.getInputRR().getReferenceableColumnAliases(null, -1);
    }
  }

  /**
   * Factory method to get DefaultExprProcessor.
   *
   * @return DefaultExprProcessor.
   */
  public DefaultExprProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }

  /**
   * Processor for subquery expressions..
   */
  public static class SubQueryExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ASTNode expr = (ASTNode) nd;
      ASTNode sqNode = (ASTNode) expr.getParent().getChild(1);

      if (!ctx.getallowSubQueryExpr()) {
        throw new SemanticException(SemanticAnalyzer.generateErrorMessage(sqNode,
            ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg()));
      }

      ExprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      /*
       * Restriction.1.h :: SubQueries only supported in the SQL Where Clause.
       */
      ctx.setError(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(sqNode,
          "Currently SubQuery expressions are only allowed as Where Clause predicates"),
          sqNode);
      return null;
    }
  }

  /**
   * Factory method to get SubQueryExprProcessor.
   *
   * @return DateExprProcessor.
   */
  public SubQueryExprProcessor getSubQueryExprProcessor() {
    return new SubQueryExprProcessor();
  }
}
