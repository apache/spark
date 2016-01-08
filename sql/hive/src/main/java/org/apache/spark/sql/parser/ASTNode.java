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
import java.util.List;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.lib.Node;

public class ASTNode extends CommonTree implements Node, Serializable {
  private static final long serialVersionUID = 1L;
  private transient StringBuffer astStr;
  private transient int startIndx = -1;
  private transient int endIndx = -1;
  private transient ASTNode rootNode;
  private transient boolean isValidASTStr;

  public ASTNode() {
  }

  /**
   * Constructor.
   *
   * @param t
   *          Token for the CommonTree Node
   */
  public ASTNode(Token t) {
    super(t);
  }

  public ASTNode(ASTNode node) {
    super(node);
  }

  @Override
  public Tree dupNode() {
    return new ASTNode(this);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hive.ql.lib.Node#getChildren()
   */
  @Override
  public ArrayList<Node> getChildren() {
    if (super.getChildCount() == 0) {
      return null;
    }

    ArrayList<Node> ret_vec = new ArrayList<Node>();
    for (int i = 0; i < super.getChildCount(); ++i) {
      ret_vec.add((Node) super.getChild(i));
    }

    return ret_vec;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hive.ql.lib.Node#getName()
   */
  @Override
  public String getName() {
    return (Integer.valueOf(super.getToken().getType())).toString();
  }

  public String dump() {
    StringBuilder sb = new StringBuilder("\n");
    dump(sb, "");
    return sb.toString();
  }

  private StringBuilder dump(StringBuilder sb, String ws) {
    sb.append(ws);
    sb.append(toString());
    sb.append("\n");

    ArrayList<Node> children = getChildren();
    if (children != null) {
      for (Node node : getChildren()) {
        if (node instanceof ASTNode) {
          ((ASTNode) node).dump(sb, ws + "   ");
        } else {
          sb.append(ws);
          sb.append("   NON-ASTNODE!!");
          sb.append("\n");
        }
      }
    }
    return sb;
  }

  private ASTNode getRootNodeWithValidASTStr(boolean useMemoizedRoot) {
    if (useMemoizedRoot && rootNode != null && rootNode.parent == null &&
        rootNode.hasValidMemoizedString()) {
      return rootNode;
    }
    ASTNode retNode = this;
    while (retNode.parent != null) {
      retNode = (ASTNode) retNode.parent;
    }
    rootNode=retNode;
    if (!rootNode.isValidASTStr) {
      rootNode.astStr = new StringBuffer();
      rootNode.toStringTree(rootNode);
      rootNode.isValidASTStr = true;
    }
    return retNode;
  }

  private boolean hasValidMemoizedString() {
    return isValidASTStr && astStr != null;
  }

  private void resetRootInformation() {
    // Reset the previously stored rootNode string
    if (rootNode != null) {
      rootNode.astStr = null;
      rootNode.isValidASTStr = false;
    }
  }

  private int getMemoizedStringLen() {
    return astStr == null ? 0 : astStr.length();
  }

  private String getMemoizedSubString(int start, int end) {
    return  (astStr == null || start < 0 || end > astStr.length() || start >= end) ? null :
      astStr.subSequence(start, end).toString();
  }

  private void addtoMemoizedString(String string) {
    if (astStr == null) {
      astStr = new StringBuffer();
    }
    astStr.append(string);
  }

  @Override
  public void setParent(Tree t) {
    super.setParent(t);
    resetRootInformation();
  }

  @Override
  public void addChild(Tree t) {
    super.addChild(t);
    resetRootInformation();
  }

  @Override
  public void addChildren(List kids) {
    super.addChildren(kids);
    resetRootInformation();
  }

  @Override
  public void setChild(int i, Tree t) {
    super.setChild(i, t);
    resetRootInformation();
  }

  @Override
  public void insertChild(int i, Object t) {
    super.insertChild(i, t);
    resetRootInformation();
  }

  @Override
  public Object deleteChild(int i) {
   Object ret = super.deleteChild(i);
   resetRootInformation();
   return ret;
  }

  @Override
  public void replaceChildren(int startChildIndex, int stopChildIndex, Object t) {
    super.replaceChildren(startChildIndex, stopChildIndex, t);
    resetRootInformation();
  }

  @Override
  public String toStringTree() {

    // The root might have changed because of tree modifications.
    // Compute the new root for this tree and set the astStr.
    getRootNodeWithValidASTStr(true);

    // If rootNotModified is false, then startIndx and endIndx will be stale.
    if (startIndx >= 0 && endIndx <= rootNode.getMemoizedStringLen()) {
      return rootNode.getMemoizedSubString(startIndx, endIndx);
    }
    return toStringTree(rootNode);
  }

  private String toStringTree(ASTNode rootNode) {
    this.rootNode = rootNode;
    startIndx = rootNode.getMemoizedStringLen();
    // Leaf node
    if ( children==null || children.size()==0 ) {
      rootNode.addtoMemoizedString(this.toString());
      endIndx =  rootNode.getMemoizedStringLen();
      return this.toString();
    }
    if ( !isNil() ) {
      rootNode.addtoMemoizedString("(");
      rootNode.addtoMemoizedString(this.toString());
      rootNode.addtoMemoizedString(" ");
    }
    for (int i = 0; children!=null && i < children.size(); i++) {
      ASTNode t = (ASTNode)children.get(i);
      if ( i>0 ) {
        rootNode.addtoMemoizedString(" ");
      }
      t.toStringTree(rootNode);
    }
    if ( !isNil() ) {
      rootNode.addtoMemoizedString(")");
    }
    endIndx =  rootNode.getMemoizedStringLen();
    return rootNode.getMemoizedSubString(startIndx, endIndx);
  }
}
