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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SemanticAnalyzer.
 *
 */
public abstract class SemanticAnalyzer {
  protected static final Logger STATIC_LOG = LoggerFactory.getLogger(SemanticAnalyzer.class.getName());
  protected final Hive db;
  protected final HiveConf conf;
  protected final Logger LOG;

  public SemanticAnalyzer(HiveConf conf, Hive db) throws SemanticException {
    try {
      this.conf = conf;
      this.db = db;
      LOG = LoggerFactory.getLogger(this.getClass().getName());
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  public static String stripIdentifierQuotes(String val) {
    if ((val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`')) {
      val = val.substring(1, val.length() - 1);
    }
    return val;
  }

  public static String stripQuotes(String val) {
    return PlanUtils.stripQuotes(val);
  }

  public static String charSetString(String charSetName, String charSetString)
      throws SemanticException {
    try {
      // The character set name starts with a _, so strip that
      charSetName = charSetName.substring(1);
      if (charSetString.charAt(0) == '\'') {
        return new String(unescapeSQLString(charSetString).getBytes(),
            charSetName);
      } else // hex input is also supported
      {
        assert charSetString.charAt(0) == '0';
        assert charSetString.charAt(1) == 'x';
        charSetString = charSetString.substring(2);

        byte[] bArray = new byte[charSetString.length() / 2];
        int j = 0;
        for (int i = 0; i < charSetString.length(); i += 2) {
          int val = Character.digit(charSetString.charAt(i), 16) * 16
              + Character.digit(charSetString.charAt(i + 1), 16);
          if (val > 127) {
            val = val - 256;
          }
          bArray[j++] = (byte)val;
        }

        String res = new String(bArray, charSetName);
        return res;
      }
    } catch (UnsupportedEncodingException e) {
      throw new SemanticException(e);
    }
  }

  /**
   * Get dequoted name from a table/column node.
   * @param tableOrColumnNode the table or column node
   * @return for table node, db.tab or tab. for column node column.
   */
  public static String getUnescapedName(ASTNode tableOrColumnNode) {
    return getUnescapedName(tableOrColumnNode, null);
  }

  public static Map.Entry<String,String> getDbTableNamePair(ASTNode tableNameNode) {
    assert(tableNameNode.getToken().getType() == SparkSqlParser.TOK_TABNAME);
    if (tableNameNode.getChildCount() == 2) {
      String dbName = unescapeIdentifier(tableNameNode.getChild(0).getText());
      String tableName = unescapeIdentifier(tableNameNode.getChild(1).getText());
      return Pair.of(dbName, tableName);
    } else {
      String tableName = unescapeIdentifier(tableNameNode.getChild(0).getText());
      return Pair.of(null,tableName);
    }
  }

  public static String getUnescapedName(ASTNode tableOrColumnNode, String currentDatabase) {
    int tokenType = tableOrColumnNode.getToken().getType();
    if (tokenType == SparkSqlParser.TOK_TABNAME) {
      // table node
      Map.Entry<String,String> dbTablePair = getDbTableNamePair(tableOrColumnNode);
      String dbName = dbTablePair.getKey();
      String tableName = dbTablePair.getValue();
      if (dbName != null){
        return dbName + "." + tableName;
      }
      if (currentDatabase != null) {
        return currentDatabase + "." + tableName;
      }
      return tableName;
    } else if (tokenType == SparkSqlParser.StringLiteral) {
      return unescapeSQLString(tableOrColumnNode.getText());
    }
    // column node
    return unescapeIdentifier(tableOrColumnNode.getText());
  }

  /**
   * Remove the encapsulating "`" pair from the identifier. We allow users to
   * use "`" to escape identifier for table names, column names and aliases, in
   * case that coincide with Hive language keywords.
   */
  public static String unescapeIdentifier(String val) {
    if (val == null) {
      return null;
    }
    if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
      val = val.substring(1, val.length() - 1);
    }
    return val;
  }

  /**
   * Converts parsed key/value properties pairs into a map.
   *
   * @param prop ASTNode parent of the key/value pairs
   *
   * @param mapProp property map which receives the mappings
   */
  public static void readProps(
    ASTNode prop, Map<String, String> mapProp) {

    for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
      String key = unescapeSQLString(prop.getChild(propChild).getChild(0)
          .getText());
      String value = null;
      if (prop.getChild(propChild).getChild(1) != null) {
        value = unescapeSQLString(prop.getChild(propChild).getChild(1).getText());
      }
      mapProp.put(key, value);
    }
  }

  private static final int[] multiplier = new int[] {1000, 100, 10, 1};

  @SuppressWarnings("nls")
  public static String unescapeSQLString(String b) {
    Character enclosure = null;

    // Some of the strings can be passed in as unicode. For example, the
    // delimiter can be passed in as \002 - So, we first check if the
    // string is a unicode number, else go back to the old behavior
    StringBuilder sb = new StringBuilder(b.length());
    for (int i = 0; i < b.length(); i++) {

      char currentChar = b.charAt(i);
      if (enclosure == null) {
        if (currentChar == '\'' || b.charAt(i) == '\"') {
          enclosure = currentChar;
        }
        // ignore all other chars outside the enclosure
        continue;
      }

      if (enclosure.equals(currentChar)) {
        enclosure = null;
        continue;
      }

      if (currentChar == '\\' && (i + 6 < b.length()) && b.charAt(i + 1) == 'u') {
        int code = 0;
        int base = i + 2;
        for (int j = 0; j < 4; j++) {
          int digit = Character.digit(b.charAt(j + base), 16);
          code += digit * multiplier[j];
        }
        sb.append((char)code);
        i += 5;
        continue;
      }

      if (currentChar == '\\' && (i + 4 < b.length())) {
        char i1 = b.charAt(i + 1);
        char i2 = b.charAt(i + 2);
        char i3 = b.charAt(i + 3);
        if ((i1 >= '0' && i1 <= '1') && (i2 >= '0' && i2 <= '7')
            && (i3 >= '0' && i3 <= '7')) {
          byte bVal = (byte) ((i3 - '0') + ((i2 - '0') * 8) + ((i1 - '0') * 8 * 8));
          byte[] bValArr = new byte[1];
          bValArr[0] = bVal;
          String tmp = new String(bValArr);
          sb.append(tmp);
          i += 3;
          continue;
        }
      }

      if (currentChar == '\\' && (i + 2 < b.length())) {
        char n = b.charAt(i + 1);
        switch (n) {
        case '0':
          sb.append("\0");
          break;
        case '\'':
          sb.append("'");
          break;
        case '"':
          sb.append("\"");
          break;
        case 'b':
          sb.append("\b");
          break;
        case 'n':
          sb.append("\n");
          break;
        case 'r':
          sb.append("\r");
          break;
        case 't':
          sb.append("\t");
          break;
        case 'Z':
          sb.append("\u001A");
          break;
        case '\\':
          sb.append("\\");
          break;
        // The following 2 lines are exactly what MySQL does TODO: why do we do this?
        case '%':
          sb.append("\\%");
          break;
        case '_':
          sb.append("\\_");
          break;
        default:
          sb.append(n);
        }
        i++;
      } else {
        sb.append(currentChar);
      }
    }
    return sb.toString();
  }

  /**
   * Escapes the string for AST; doesn't enclose it in quotes, however.
   */
  public static String escapeSQLString(String b) {
    // There's usually nothing to escape so we will be optimistic.
    String result = b;
    for (int i = 0; i < result.length(); ++i) {
      char currentChar = result.charAt(i);
      if (currentChar == '\\' && ((i + 1) < result.length())) {
        // TODO: do we need to handle the "this is what MySQL does" here?
        char nextChar = result.charAt(i + 1);
        if (nextChar == '%' || nextChar == '_') {
          ++i;
          continue;
        }
      }
      switch (currentChar) {
      case '\0':
        result = spliceString(result, i, "\\0");
        ++i;
        break;
      case '\'':
        result = spliceString(result, i, "\\'");
        ++i;
        break;
      case '\"':
        result = spliceString(result, i, "\\\"");
        ++i;
        break;
      case '\b':
        result = spliceString(result, i, "\\b");
        ++i;
        break;
      case '\n':
        result = spliceString(result, i, "\\n");
        ++i;
        break;
      case '\r':
        result = spliceString(result, i, "\\r");
        ++i;
        break;
      case '\t':
        result = spliceString(result, i, "\\t");
        ++i;
        break;
      case '\\':
        result = spliceString(result, i, "\\\\");
        ++i;
        break;
      case '\u001A':
        result = spliceString(result, i, "\\Z");
        ++i;
        break;
      default: {
        if (currentChar < ' ') {
          String hex = Integer.toHexString(currentChar);
          String unicode = "\\u";
          for (int j = 4; j > hex.length(); --j) {
            unicode += '0';
          }
          unicode += hex;
          result = spliceString(result, i, unicode);
          i += (unicode.length() - 1);
        }
        break; // if not a control character, do nothing
      }
      }
    }
    return result;
  }

  private static String spliceString(String str, int i, String replacement) {
    return spliceString(str, i, 1, replacement);
  }

  private static String spliceString(String str, int i, int length, String replacement) {
    return str.substring(0, i) + replacement + str.substring(i + length);
  }

  /**
   * Get the list of FieldSchema out of the ASTNode.
   */
  public static List<FieldSchema> getColumns(ASTNode ast, boolean lowerCase) throws SemanticException {
    List<FieldSchema> colList = new ArrayList<FieldSchema>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      FieldSchema col = new FieldSchema();
      ASTNode child = (ASTNode) ast.getChild(i);
      Tree grandChild = child.getChild(0);
      if(grandChild != null) {
        String name = grandChild.getText();
        if(lowerCase) {
          name = name.toLowerCase();
        }
        // child 0 is the name of the column
        col.setName(unescapeIdentifier(name));
        // child 1 is the type of the column
        ASTNode typeChild = (ASTNode) (child.getChild(1));
        col.setType(getTypeStringFromAST(typeChild));

        // child 2 is the optional comment of the column
        if (child.getChildCount() == 3) {
          col.setComment(unescapeSQLString(child.getChild(2).getText()));
        }
      }
      colList.add(col);
    }
    return colList;
  }

  protected static String getTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    switch (typeNode.getType()) {
    case SparkSqlParser.TOK_LIST:
      return serdeConstants.LIST_TYPE_NAME + "<"
          + getTypeStringFromAST((ASTNode) typeNode.getChild(0)) + ">";
    case SparkSqlParser.TOK_MAP:
      return serdeConstants.MAP_TYPE_NAME + "<"
          + getTypeStringFromAST((ASTNode) typeNode.getChild(0)) + ","
          + getTypeStringFromAST((ASTNode) typeNode.getChild(1)) + ">";
    case SparkSqlParser.TOK_STRUCT:
      return getStructTypeStringFromAST(typeNode);
    case SparkSqlParser.TOK_UNIONTYPE:
      return getUnionTypeStringFromAST(typeNode);
    default:
      return getTypeName(typeNode);
    }
  }

  private static String getStructTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    String typeStr = serdeConstants.STRUCT_TYPE_NAME + "<";
    typeNode = (ASTNode) typeNode.getChild(0);
    int children = typeNode.getChildCount();
    if (children <= 0) {
      throw new SemanticException("empty struct not allowed.");
    }
    StringBuilder buffer = new StringBuilder(typeStr);
    for (int i = 0; i < children; i++) {
      ASTNode child = (ASTNode) typeNode.getChild(i);
      buffer.append(unescapeIdentifier(child.getChild(0).getText())).append(":");
      buffer.append(getTypeStringFromAST((ASTNode) child.getChild(1)));
      if (i < children - 1) {
        buffer.append(",");
      }
    }

    buffer.append(">");
    return buffer.toString();
  }

  private static String getUnionTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    String typeStr = serdeConstants.UNION_TYPE_NAME + "<";
    typeNode = (ASTNode) typeNode.getChild(0);
    int children = typeNode.getChildCount();
    if (children <= 0) {
      throw new SemanticException("empty union not allowed.");
    }
    StringBuilder buffer = new StringBuilder(typeStr);
    for (int i = 0; i < children; i++) {
      buffer.append(getTypeStringFromAST((ASTNode) typeNode.getChild(i)));
      if (i < children - 1) {
        buffer.append(",");
      }
    }
    buffer.append(">");
    typeStr = buffer.toString();
    return typeStr;
  }

  public Hive getDb() {
    return db;
  }

  /**
   * Given a ASTNode, return list of values.
   *
   * use case:
   *   create table xyz list bucketed (col1) with skew (1,2,5)
   *   AST Node is for (1,2,5)
   * @param ast
   * @return
   */
  public static List<String> getSkewedValueFromASTNode(ASTNode ast) {
    List<String> colList = new ArrayList<String>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      colList.add(stripQuotes(child.getText()).toLowerCase());
    }
    return colList;
  }

  /**
   * Retrieve skewed values from ASTNode.
   *
   * @param node
   * @return
   * @throws SemanticException
   */
  public static List<String> getSkewedValuesFromASTNode(Node node) throws SemanticException {
    List<String> result = null;
    Tree leafVNode = ((ASTNode) node).getChild(0);
    if (leafVNode == null) {
      throw new SemanticException(
          ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
    } else {
      ASTNode lVAstNode = (ASTNode) leafVNode;
      if (lVAstNode.getToken().getType() != SparkSqlParser.TOK_TABCOLVALUE) {
        throw new SemanticException(
            ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
      } else {
        result = new ArrayList<String>(getSkewedValueFromASTNode(lVAstNode));
      }
    }
    return result;
  }

  private static boolean getPartExprNodeDesc(ASTNode astNode, HiveConf conf,
      Map<ASTNode, ExprNodeDesc> astExprNodeMap) throws SemanticException {

    if (astNode == null) {
      return true;
    } else if ((astNode.getChildren() == null) || (astNode.getChildren().size() == 0)) {
      return astNode.getType() != SparkSqlParser.TOK_PARTVAL;
    }

    TypeCheckCtx typeCheckCtx = new TypeCheckCtx(null);
    String defaultPartitionName = HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME);
    boolean result = true;
    for (Node childNode : astNode.getChildren()) {
      ASTNode childASTNode = (ASTNode)childNode;

      if (childASTNode.getType() != SparkSqlParser.TOK_PARTVAL) {
        result = getPartExprNodeDesc(childASTNode, conf, astExprNodeMap) && result;
      } else {
        boolean isDynamicPart = childASTNode.getChildren().size() <= 1;
        result = !isDynamicPart && result;
        if (!isDynamicPart) {
          ASTNode partVal = (ASTNode)childASTNode.getChildren().get(1);
          if (!defaultPartitionName.equalsIgnoreCase(unescapeSQLString(partVal.getText()))) {
            astExprNodeMap.put((ASTNode)childASTNode.getChildren().get(0),
                TypeCheckProcFactory.genExprNode(partVal, typeCheckCtx).get(partVal));
          }
        }
      }
    }
    return result;
  }

  public static void validatePartSpec(Table tbl, Map<String, String> partSpec,
      ASTNode astNode, HiveConf conf, boolean shouldBeFull) throws SemanticException {
    tbl.validatePartColumnNames(partSpec, shouldBeFull);
    validatePartColumnType(tbl, partSpec, astNode, conf);
  }

  public static void validatePartColumnType(Table tbl, Map<String, String> partSpec,
      ASTNode astNode, HiveConf conf) throws SemanticException {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_TYPE_CHECK_ON_INSERT)) {
      return;
    }

    Map<ASTNode, ExprNodeDesc> astExprNodeMap = new HashMap<ASTNode, ExprNodeDesc>();
    if (!getPartExprNodeDesc(astNode, conf, astExprNodeMap)) {
      STATIC_LOG.warn("Dynamic partitioning is used; only validating "
          + astExprNodeMap.size() + " columns");
    }

    if (astExprNodeMap.isEmpty()) {
      return; // All columns are dynamic, nothing to do.
    }

    List<FieldSchema> parts = tbl.getPartitionKeys();
    Map<String, String> partCols = new HashMap<String, String>(parts.size());
    for (FieldSchema col : parts) {
      partCols.put(col.getName(), col.getType().toLowerCase());
    }
    for (Entry<ASTNode, ExprNodeDesc> astExprNodePair : astExprNodeMap.entrySet()) {
      String astKeyName = astExprNodePair.getKey().toString().toLowerCase();
      if (astExprNodePair.getKey().getType() == SparkSqlParser.Identifier) {
        astKeyName = stripIdentifierQuotes(astKeyName);
      }
      String colType = partCols.get(astKeyName);
      ObjectInspector inputOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
          astExprNodePair.getValue().getTypeInfo());

      TypeInfo expectedType =
          TypeInfoUtils.getTypeInfoFromTypeString(colType);
      ObjectInspector outputOI =
          TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(expectedType);
      //  Since partVal is a constant, it is safe to cast ExprNodeDesc to ExprNodeConstantDesc.
      //  Its value should be in normalized format (e.g. no leading zero in integer, date is in
      //  format of YYYY-MM-DD etc)
      Object value = ((ExprNodeConstantDesc)astExprNodePair.getValue()).getValue();
      Object convertedValue = value;
      if (!inputOI.getTypeName().equals(outputOI.getTypeName())) {
        convertedValue = ObjectInspectorConverters.getConverter(inputOI, outputOI).convert(value);
        if (convertedValue == null) {
          throw new SemanticException(ErrorMsg.PARTITION_SPEC_TYPE_MISMATCH, astKeyName,
              inputOI.getTypeName(), outputOI.getTypeName());
        }

        if (!convertedValue.toString().equals(value.toString())) {
          //  value might have been changed because of the normalization in conversion
          STATIC_LOG.warn("Partition " + astKeyName + " expects type " + outputOI.getTypeName()
          + " but input value is in type " + inputOI.getTypeName() + ". Convert "
          + value.toString() + " to " + convertedValue.toString());
        }
      }

      if (!convertedValue.toString().equals(partSpec.get(astKeyName))) {
        STATIC_LOG.warn("Partition Spec " + astKeyName + "=" + partSpec.get(astKeyName)
            + " has been changed to " + astKeyName + "=" + convertedValue.toString());
      }
      partSpec.put(astKeyName, convertedValue.toString());
    }
  }

  private Path tryQualifyPath(Path path) throws IOException {
    try {
      return path.getFileSystem(conf).makeQualified(path);
    } catch (IOException e) {
      return path;  // some tests expected to pass invalid schema
    }
  }

  protected String toMessage(ErrorMsg message, Object detail) {
    return detail == null ? message.getMsg() : message.getMsg(detail.toString());
  }

  public static String getAstNodeText(ASTNode tree) {
    return tree.getChildCount() == 0?tree.getText() :
        getAstNodeText((ASTNode)tree.getChild(tree.getChildCount() - 1));
  }

  public static String generateErrorMessage(ASTNode ast, String message) {
    StringBuilder sb = new StringBuilder();
    if (ast == null) {
      sb.append(message).append(". Cannot tell the position of null AST.");
      return sb.toString();
    }
    sb.append(ast.getLine());
    sb.append(":");
    sb.append(ast.getCharPositionInLine());
    sb.append(" ");
    sb.append(message);
    sb.append(". Error encountered near token '");
    sb.append(getAstNodeText(ast));
    sb.append("'");
    return sb.toString();
  }

  public static String getColumnInternalName(int pos) {
    return HiveConf.getColumnInternalName(pos);
  }

  private static final Map<Integer, String> TokenToTypeName = new HashMap<Integer, String>();

  static {
    TokenToTypeName.put(SparkSqlParser.TOK_BOOLEAN, serdeConstants.BOOLEAN_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_TINYINT, serdeConstants.TINYINT_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_SMALLINT, serdeConstants.SMALLINT_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_INT, serdeConstants.INT_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_BIGINT, serdeConstants.BIGINT_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_FLOAT, serdeConstants.FLOAT_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_DOUBLE, serdeConstants.DOUBLE_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_STRING, serdeConstants.STRING_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_CHAR, serdeConstants.CHAR_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_VARCHAR, serdeConstants.VARCHAR_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_BINARY, serdeConstants.BINARY_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_DATE, serdeConstants.DATE_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_DATETIME, serdeConstants.DATETIME_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_TIMESTAMP, serdeConstants.TIMESTAMP_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_INTERVAL_YEAR_MONTH, serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_INTERVAL_DAY_TIME, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
    TokenToTypeName.put(SparkSqlParser.TOK_DECIMAL, serdeConstants.DECIMAL_TYPE_NAME);
  }

  public static String getTypeName(ASTNode node) throws SemanticException {
    int token = node.getType();
    String typeName;

    // datetime type isn't currently supported
    if (token == SparkSqlParser.TOK_DATETIME) {
      throw new SemanticException(ErrorMsg.UNSUPPORTED_TYPE.getMsg());
    }

    switch (token) {
      case SparkSqlParser.TOK_CHAR:
        CharTypeInfo charTypeInfo = ParseUtils.getCharTypeInfo(node);
        typeName = charTypeInfo.getQualifiedName();
        break;
      case SparkSqlParser.TOK_VARCHAR:
        VarcharTypeInfo varcharTypeInfo = ParseUtils.getVarcharTypeInfo(node);
        typeName = varcharTypeInfo.getQualifiedName();
        break;
      case SparkSqlParser.TOK_DECIMAL:
        DecimalTypeInfo decTypeInfo = ParseUtils.getDecimalTypeTypeInfo(node);
        typeName = decTypeInfo.getQualifiedName();
        break;
      default:
        typeName = TokenToTypeName.get(token);
    }
    return typeName;
  }

  public static String relativeToAbsolutePath(HiveConf conf, String location) throws SemanticException {
    boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
    if (testMode) {
      URI uri = new Path(location).toUri();
      String scheme = uri.getScheme();
      String authority = uri.getAuthority();
      String path = uri.getPath();
      if (!path.startsWith("/")) {
        path = (new Path(System.getProperty("test.tmp.dir"),
            path)).toUri().getPath();
      }
      if (StringUtils.isEmpty(scheme)) {
        scheme = "pfile";
      }
      try {
        uri = new URI(scheme, authority, path, null, null);
      } catch (URISyntaxException e) {
        throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
      }
      return uri.toString();
    } else {
      //no-op for non-test mode for now
      return location;
    }
  }
}
