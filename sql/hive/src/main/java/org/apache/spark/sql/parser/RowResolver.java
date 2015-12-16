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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the Row Resolver.
 *
 */
public class RowResolver implements Serializable{
  private static final long serialVersionUID = 1L;
  private  RowSchema rowSchema;
  private  HashMap<String, LinkedHashMap<String, ColumnInfo>> rslvMap;

  private  HashMap<String, String[]> invRslvMap;
  /*
   * now a Column can have an alternate mapping.
   * This captures the alternate mapping.
   * The primary(first) mapping is still only held in
   * invRslvMap.
   */
  private final Map<String, String[]> altInvRslvMap;
  private  Map<String, ASTNode> expressionMap;

  // TODO: Refactor this and do in a more object oriented manner
  private boolean isExprResolver;

  private static final Logger LOG = LoggerFactory.getLogger(RowResolver.class.getName());

  public RowResolver() {
    rowSchema = new RowSchema();
    rslvMap = new HashMap<String, LinkedHashMap<String, ColumnInfo>>();
    invRslvMap = new HashMap<String, String[]>();
    altInvRslvMap = new HashMap<String, String[]>();
    expressionMap = new HashMap<String, ASTNode>();
    isExprResolver = false;
  }

  /**
   * Puts a resolver entry corresponding to a source expression which is to be
   * used for identical expression recognition (e.g. for matching expressions
   * in the SELECT list with the GROUP BY clause).  The convention for such
   * entries is an empty-string ("") as the table alias together with the
   * string rendering of the ASTNode as the column alias.
   */
  public void putExpression(ASTNode node, ColumnInfo colInfo) {
    String treeAsString = node.toStringTree();
    expressionMap.put(treeAsString, node);
    put("", treeAsString, colInfo);
  }

  /**
   * Retrieves the ColumnInfo corresponding to a source expression which
   * exactly matches the string rendering of the given ASTNode.
   */
  public ColumnInfo getExpression(ASTNode node) throws SemanticException {
    return get("", node.toStringTree());
  }

  /**
   * Retrieves the source expression matching a given ASTNode's
   * string rendering exactly.
   */
  public ASTNode getExpressionSource(ASTNode node) {
    return expressionMap.get(node.toStringTree());
  }

  public void put(String tab_alias, String col_alias, ColumnInfo colInfo) {
    if (!addMappingOnly(tab_alias, col_alias, colInfo)) {
      //Make sure that the table alias and column alias are stored
      //in the column info
      if (tab_alias != null) {
        colInfo.setTabAlias(tab_alias.toLowerCase());
      }
      if (col_alias != null) {
        colInfo.setAlias(col_alias.toLowerCase());
      }
      rowSchema.getSignature().add(colInfo);
    }
  }

  public boolean addMappingOnly(String tab_alias, String col_alias, ColumnInfo colInfo) {
    if (tab_alias != null) {
      tab_alias = tab_alias.toLowerCase();
    }
    col_alias = col_alias.toLowerCase();

    /*
     * allow multiple mappings to the same ColumnInfo.
     * When a ColumnInfo is mapped multiple times, only the
     * first inverse mapping is captured.
     */
    boolean colPresent = invRslvMap.containsKey(colInfo.getInternalName());

    LinkedHashMap<String, ColumnInfo> f_map = rslvMap.get(tab_alias);
    if (f_map == null) {
      f_map = new LinkedHashMap<String, ColumnInfo>();
      rslvMap.put(tab_alias, f_map);
    }
    ColumnInfo oldColInfo = f_map.put(col_alias, colInfo);
    if (oldColInfo != null) {
      LOG.warn("Duplicate column info for " + tab_alias + "." + col_alias
          + " was overwritten in RowResolver map: " + oldColInfo + " by " + colInfo);
    }

    String[] qualifiedAlias = new String[2];
    qualifiedAlias[0] = tab_alias;
    qualifiedAlias[1] = col_alias;
    if ( !colPresent ) {
      invRslvMap.put(colInfo.getInternalName(), qualifiedAlias);
    } else {
      altInvRslvMap.put(colInfo.getInternalName(), qualifiedAlias);
    }

    return colPresent;
  }

  public boolean hasTableAlias(String tab_alias) {
    return rslvMap.get(tab_alias.toLowerCase()) != null;
  }

  /**
   * Gets the column Info to tab_alias.col_alias type of a column reference. I
   * the tab_alias is not provided as can be the case with an non aliased
   * column, this function looks up the column in all the table aliases in this
   * row resolver and returns the match. It also throws an exception if the
   * column is found in multiple table aliases. If no match is found a null
   * values is returned.
   *
   * This allows us to interpret both select t.c1 type of references and select
   * c1 kind of references. The later kind are what we call non aliased column
   * references in the query.
   *
   * @param tab_alias
   *          The table alias to match (this is null if the column reference is
   *          non aliased)
   * @param col_alias
   *          The column name that is being searched for
   * @return ColumnInfo
   * @throws SemanticException
   */
  public ColumnInfo get(String tab_alias, String col_alias) throws SemanticException {
    col_alias = col_alias.toLowerCase();
    ColumnInfo ret = null;

    if (tab_alias != null) {
      tab_alias = tab_alias.toLowerCase();
      HashMap<String, ColumnInfo> f_map = rslvMap.get(tab_alias);
      if (f_map == null) {
        return null;
      }
      ret = f_map.get(col_alias);
    } else {
      boolean found = false;
      String foundTbl = null;
      for (Map.Entry<String, LinkedHashMap<String, ColumnInfo>> rslvEntry: rslvMap.entrySet()) {
        String rslvKey = rslvEntry.getKey();
        LinkedHashMap<String, ColumnInfo> cmap = rslvEntry.getValue();
        for (Map.Entry<String, ColumnInfo> cmapEnt : cmap.entrySet()) {
          if (col_alias.equalsIgnoreCase(cmapEnt.getKey())) {
            /*
             * We can have an unaliased and one aliased mapping to a Column.
             */
            if (found && foundTbl != null && rslvKey != null) {
              throw new SemanticException("Column " + col_alias
                  + " Found in more than One Tables/Subqueries");
            }
            found = true;
            foundTbl = rslvKey == null ? foundTbl : rslvKey;
            ret = cmapEnt.getValue();
          }
        }
      }
    }

    return ret;
  }

  public ArrayList<ColumnInfo> getColumnInfos() {
    return rowSchema.getSignature();
  }

  /**
   * Get a list of aliases for non-hidden columns
   * @param max the maximum number of columns to return
   * @return a list of non-hidden column names no greater in size than max
   */
  public List<String> getReferenceableColumnAliases(String tableAlias, int max) {
    int count = 0;
    Set<String> columnNames = new LinkedHashSet<String>();

    int tables = rslvMap.size();

    Map<String, ColumnInfo> mapping = rslvMap.get(tableAlias);
    if (mapping != null) {
      for (Map.Entry<String, ColumnInfo> entry : mapping.entrySet()) {
        if (max > 0 && count >= max) {
          break;
        }
        ColumnInfo columnInfo = entry.getValue();
        if (!columnInfo.isHiddenVirtualCol()) {
          columnNames.add(entry.getKey());
          count++;
        }
      }
    } else {
      for (ColumnInfo columnInfo : getColumnInfos()) {
        if (max > 0 && count >= max) {
          break;
        }
        if (!columnInfo.isHiddenVirtualCol()) {
          String[] inverse = !isExprResolver ? reverseLookup(columnInfo.getInternalName()) : null;
          if (inverse != null) {
            columnNames.add(inverse[0] == null || tables <= 1 ? inverse[1] :
                inverse[0] + "." + inverse[1]);
          } else {
            columnNames.add(columnInfo.getAlias());
          }
          count++;
        }
      }
    }
    return new ArrayList<String>(columnNames);
  }

  public HashMap<String, ColumnInfo> getFieldMap(String tabAlias) {
    if (tabAlias == null) {
      return rslvMap.get(null);
    } else {
      return rslvMap.get(tabAlias.toLowerCase());
    }
  }

  public String[] reverseLookup(String internalName) {
    return invRslvMap.get(internalName);
  }

  public boolean getIsExprResolver() {
    return isExprResolver;
  }

  public String[] getAlternateMappings(String internalName) {
    return altInvRslvMap.get(internalName);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    for (Map.Entry<String, LinkedHashMap<String, ColumnInfo>> e : rslvMap
        .entrySet()) {
      String tab = e.getKey();
      sb.append(tab + "{");
      HashMap<String, ColumnInfo> f_map = e.getValue();
      if (f_map != null) {
        for (Map.Entry<String, ColumnInfo> entry : f_map.entrySet()) {
          sb.append("(" + entry.getKey() + "," + entry.getValue().toString()
              + ")");
        }
      }
      sb.append("} ");
    }
    return sb.toString();
  }

  public RowSchema getRowSchema() {
    return rowSchema;
  }

  private static class IntRef {
    public int val = 0;
  }

  public static boolean add(RowResolver rrToAddTo, RowResolver rrToAddFrom, int numColumns)
      throws SemanticException {
    return add(rrToAddTo, rrToAddFrom, null, numColumns);
  }

  // TODO: 1) How to handle collisions? 2) Should we be cloning ColumnInfo or not?
  private static boolean add(RowResolver rrToAddTo, RowResolver rrToAddFrom,
      IntRef outputColPosRef, int numColumns) throws SemanticException {
    boolean hasDuplicates = false;
    String tabAlias;
    String colAlias;
    String[] qualifiedColName;
    int i = 0;

    int outputColPos = outputColPosRef == null ? 0 : outputColPosRef.val;
    for (ColumnInfo cInfoFrmInput : rrToAddFrom.getRowSchema().getSignature()) {
      if ( numColumns >= 0 && i == numColumns ) {
        break;
      }
      ColumnInfo newCI = null;
      String internalName = cInfoFrmInput.getInternalName();
      qualifiedColName = rrToAddFrom.reverseLookup(internalName);
      tabAlias = qualifiedColName[0];
      colAlias = qualifiedColName[1];

      newCI = new ColumnInfo(cInfoFrmInput);
      newCI.setInternalName(SemanticAnalyzer.getColumnInternalName(outputColPos));

      outputColPos++;

      boolean isUnique = rrToAddTo.putWithCheck(tabAlias, colAlias, internalName, newCI);
      hasDuplicates |= (!isUnique);

      qualifiedColName = rrToAddFrom.getAlternateMappings(internalName);
      if (qualifiedColName != null) {
        tabAlias = qualifiedColName[0];
        colAlias = qualifiedColName[1];
        rrToAddTo.put(tabAlias, colAlias, newCI);
      }
      i++;
    }

    if (outputColPosRef != null) {
      outputColPosRef.val = outputColPos;
    }
    return !hasDuplicates;
  }

  /**
   * Adds column to RR, checking for duplicate columns. Needed because CBO cannot handle the Hive
   * behavior of blindly overwriting old mapping in RR and still somehow working after that.
   * @return True if mapping was added without duplicates.
   */
  public boolean putWithCheck(String tabAlias, String colAlias,
      String internalName, ColumnInfo newCI) throws SemanticException {
    ColumnInfo existing = get(tabAlias, colAlias);
    // Hive adds the same mapping twice... I wish we could fix stuff like that.
    if (existing == null) {
      put(tabAlias, colAlias, newCI);
      return true;
    } else if (existing.isSameColumnForRR(newCI)) {
      return true;
    }
    LOG.warn("Found duplicate column alias in RR: "
        + existing.toMappingString(tabAlias, colAlias) + " adding "
        + newCI.toMappingString(tabAlias, colAlias));
    if (internalName != null) {
      existing = get(tabAlias, internalName);
      if (existing == null) {
        put(tabAlias, internalName, newCI);
        return true;
      } else if (existing.isSameColumnForRR(newCI)) {
        return true;
      }
      LOG.warn("Failed to use internal name after finding a duplicate: "
          + existing.toMappingString(tabAlias, internalName));
    }
    return false;
  }

  public static boolean add(RowResolver rrToAddTo, RowResolver rrToAddFrom)
      throws SemanticException {
    return add(rrToAddTo, rrToAddFrom, null, -1);
  }
}
