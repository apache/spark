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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.conf.Configuration;

/**
 * UnparseTranslator is used to "unparse" objects such as views when their
 * definition is stored. It has a translations map where its possible to replace all the
 * text with the appropriate escaped version [say invites.ds will be replaced with
 * `invites`.`ds` and the entire query is processed like this and stored as
 * Extended text in table's metadata]. This holds all individual translations and
 * where they apply in the stream. The unparse is lazy and happens when
 * SemanticAnalyzer.saveViewDefinition() calls TokenRewriteStream.toString().
 *
 */
public class UnparseTranslator {
  // key is token start index
  private final NavigableMap<Integer, Translation> translations;
  private final List<CopyTranslation> copyTranslations;
  private boolean enabled;
  private Configuration conf;

  public UnparseTranslator(Configuration conf) {
    this.conf = conf;
    translations = new TreeMap<Integer, Translation>();
    copyTranslations = new ArrayList<CopyTranslation>();
  }

  /**
   * Enable this translator.
   */
  void enable() {
    enabled = true;
  }

  /**
   * @return whether this translator has been enabled
   */
  boolean isEnabled() {
    return enabled;
  }

  /**
   * Register a translation to be performed as part of unparse. ANTLR imposes
   * strict conditions on the translations and errors out during
   * TokenRewriteStream.toString() if there is an overlap. It expects all
   * the translations to be disjoint (See HIVE-2439).
   * If the translation overlaps with any previously
   * registered translation, then it must be either
   * identical or a prefix (in which cases it is ignored),
   * or else it must extend the existing translation (i.e.
   * the existing translation must be a prefix/suffix of the new translation).
   * All other overlap cases result in assertion failures.
   *
   * @param node
   *          target node whose subtree is to be replaced
   *
   * @param replacementText
   *          text to use as replacement
   */
  void addTranslation(ASTNode node, String replacementText) {
    if (!enabled) {
      return;
    }

    if (node.getOrigin() != null) {
      // This node was parsed while loading the definition of another view
      // being referenced by the one being created, and we don't want
      // to track any expansions for the underlying view.
      return;
    }

    int tokenStartIndex = node.getTokenStartIndex();
    int tokenStopIndex = node.getTokenStopIndex();
    if (tokenStopIndex < 0) {
      // this is for artificially added tokens
      return;
    }
    Translation translation = new Translation();
    translation.tokenStopIndex = tokenStopIndex;
    translation.replacementText = replacementText;

    // Sanity check for overlap with regions already being expanded
    assert (tokenStopIndex >= tokenStartIndex);

    List<Integer> subsetEntries = new ArrayList<Integer>();
    // Is the existing entry and newer entry are subset of one another ?
    for (Map.Entry<Integer, Translation> existingEntry :
          translations.headMap(tokenStopIndex, true).entrySet()) {
      // check if the new entry contains the existing
      if (existingEntry.getValue().tokenStopIndex <= tokenStopIndex &&
            existingEntry.getKey() >= tokenStartIndex) {
        // Collect newer entry is if a super-set of existing entry,
        assert (replacementText.contains(existingEntry.getValue().replacementText));
        subsetEntries.add(existingEntry.getKey());
        // check if the existing entry contains the new
      } else if (existingEntry.getValue().tokenStopIndex >= tokenStopIndex &&
            existingEntry.getKey() <= tokenStartIndex) {
        assert (existingEntry.getValue().replacementText.contains(replacementText));
        // we don't need to add this new entry since there's already an overlapping one
        return;
      }
    }
    // remove any existing entries that are contained by the new one
    for (Integer index : subsetEntries) {
      translations.remove(index);
    }

    // It's all good: create a new entry in the map (or update existing one)
    translations.put(tokenStartIndex, translation);
  }

  /**
   * Register a translation for an tabName.
   *
   * @param tableName
   *          source node (which must be an tabName) to be replaced
   */
  void addTableNameTranslation(ASTNode tableName, String currentDatabaseName) {
    if (!enabled) {
      return;
    }
    if (tableName.getToken().getType() == SparkSqlParser.Identifier) {
      addIdentifierTranslation(tableName);
      return;
    }
    assert (tableName.getToken().getType() == SparkSqlParser.TOK_TABNAME);
    assert (tableName.getChildCount() <= 2);

    if (tableName.getChildCount() == 2) {
      addIdentifierTranslation((ASTNode)tableName.getChild(0));
      addIdentifierTranslation((ASTNode)tableName.getChild(1));
    }
    else {
      // transform the table reference to an absolute reference (i.e., "db.table")
      StringBuilder replacementText = new StringBuilder();
      replacementText.append(ParseUtils.unparseIdentifier(currentDatabaseName, conf));
      replacementText.append('.');

      ASTNode identifier = (ASTNode)tableName.getChild(0);
      String identifierText = SemanticAnalyzer.unescapeIdentifier(identifier.getText());
      replacementText.append(ParseUtils.unparseIdentifier(identifierText, conf));

      addTranslation(identifier, replacementText.toString());
    }
  }

  /**
   * Register a translation for an identifier.
   *
   * @param identifier
   *          source node (which must be an identifier) to be replaced
   */
  void addIdentifierTranslation(ASTNode identifier) {
    if (!enabled) {
      return;
    }
    assert (identifier.getToken().getType() == SparkSqlParser.Identifier);
    String replacementText = identifier.getText();
    replacementText = SemanticAnalyzer.unescapeIdentifier(replacementText);
    replacementText = ParseUtils.unparseIdentifier(replacementText, conf);
    addTranslation(identifier, replacementText);
  }

  /**
   * Register a "copy" translation in which a node will be translated into
   * whatever the translation turns out to be for another node (after
   * previously registered translations have already been performed).  Deferred
   * translations are performed in the order they are registered, and follow
   * the same rules regarding overlap as non-copy translations.
   *
   * @param targetNode node whose subtree is to be replaced
   *
   * @param sourceNode the node providing the replacement text
   *
   */
  void addCopyTranslation(ASTNode targetNode, ASTNode sourceNode) {
    if (!enabled) {
      return;
    }

    if (targetNode.getOrigin() != null) {
      return;
    }

    CopyTranslation copyTranslation = new CopyTranslation();
    copyTranslation.targetNode = targetNode;
    copyTranslation.sourceNode = sourceNode;
    copyTranslations.add(copyTranslation);
  }

  /**
   * Apply all translations on the given token stream.
   *
   * @param tokenRewriteStream
   *          rewrite-capable stream
   */
  void applyTranslations(TokenRewriteStream tokenRewriteStream) {
    for (Map.Entry<Integer, Translation> entry : translations.entrySet()) {
      if (entry.getKey() > 0) {
        // negative means the key didn't exist in the original stream (i.e.: we changed the tree)
        tokenRewriteStream.replace(
           entry.getKey(),
           entry.getValue().tokenStopIndex,
           entry.getValue().replacementText);
      }
    }
    for (CopyTranslation copyTranslation : copyTranslations) {
      String replacementText = tokenRewriteStream.toString(
        copyTranslation.sourceNode.getTokenStartIndex(),
        copyTranslation.sourceNode.getTokenStopIndex());
      String currentText = tokenRewriteStream.toString(
        copyTranslation.targetNode.getTokenStartIndex(),
        copyTranslation.targetNode.getTokenStopIndex());
      if (currentText.equals(replacementText)) {
        // copy is a nop, so skip it--this is important for avoiding
        // spurious overlap assertions
        continue;
      }
      // Call addTranslation just to get the assertions for overlap
      // checking.
      addTranslation(copyTranslation.targetNode, replacementText);
      tokenRewriteStream.replace(
        copyTranslation.targetNode.getTokenStartIndex(),
        copyTranslation.targetNode.getTokenStopIndex(),
        replacementText);
    }
  }

  private static class Translation {
    int tokenStopIndex;
    String replacementText;

    @Override
    public String toString() {
      return "" + tokenStopIndex + " -> " + replacementText;
    }
  }

  private static class CopyTranslation {
    ASTNode targetNode;
    ASTNode sourceNode;
  }

  public void clear() {
    translations.clear();
    copyTranslations.clear();
    enabled = false;
  }
}
