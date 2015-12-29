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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;


/**
 * Library of utility functions used in the parse code.
 *
 */
public final class ParseUtils {
  /**
   * Performs a descent of the leftmost branch of a tree, stopping when either a
   * node with a non-null token is found or the leaf level is encountered.
   *
   * @param tree
   *          candidate node from which to start searching
   *
   * @return node at which descent stopped
   */
  public static ASTNode findRootNonNullToken(ASTNode tree) {
    while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
      tree = (org.apache.spark.sql.parser.ASTNode) tree.getChild(0);
    }
    return tree;
  }

  private ParseUtils() {
    // prevent instantiation
  }

  public static VarcharTypeInfo getVarcharTypeInfo(org.apache.spark.sql.parser.ASTNode node)
      throws SemanticException {
    if (node.getChildCount() != 1) {
      throw new SemanticException("Bad params for type varchar");
    }

    String lengthStr = node.getChild(0).getText();
    return TypeInfoFactory.getVarcharTypeInfo(Integer.valueOf(lengthStr));
  }

  public static CharTypeInfo getCharTypeInfo(org.apache.spark.sql.parser.ASTNode node)
      throws SemanticException {
    if (node.getChildCount() != 1) {
      throw new SemanticException("Bad params for type char");
    }

    String lengthStr = node.getChild(0).getText();
    return TypeInfoFactory.getCharTypeInfo(Integer.valueOf(lengthStr));
  }

  static int getIndex(String[] list, String elem) {
    for(int i=0; i < list.length; i++) {
      if (list[i] != null && list[i].toLowerCase().equals(elem)) {
        return i;
      }
    }
    return -1;
  }

  public static DecimalTypeInfo getDecimalTypeTypeInfo(org.apache.spark.sql.parser.ASTNode node)
      throws SemanticException {
    if (node.getChildCount() > 2) {
        throw new SemanticException("Bad params for type decimal");
      }

      int precision = HiveDecimal.USER_DEFAULT_PRECISION;
      int scale = HiveDecimal.USER_DEFAULT_SCALE;

      if (node.getChildCount() >= 1) {
        String precStr = node.getChild(0).getText();
        precision = Integer.valueOf(precStr);
      }

      if (node.getChildCount() == 2) {
        String scaleStr = node.getChild(1).getText();
        scale = Integer.valueOf(scaleStr);
      }

      return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
  }

  public static String ensureClassExists(String className)
      throws SemanticException {
    if (className == null) {
      return null;
    }
    try {
      Class.forName(className, true, Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException("Cannot find class '" + className + "'", e);
    }
    return className;
  }

  public static String unparseIdentifier(String identifier) {
    return unparseIdentifier(identifier, (Configuration)null);
  }

  public static String unparseIdentifier(String identifier, Configuration conf) {
    String qIdSupport = conf == null?null: HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT);
    if(qIdSupport != null && !"none".equals(qIdSupport)) {
      identifier = identifier.replaceAll("`", "``");
    }

    return "`" + identifier + "`";
  }
}
