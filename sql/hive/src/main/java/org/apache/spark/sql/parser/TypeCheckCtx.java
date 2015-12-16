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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;

/**
 * This class implements the context information that is used for typechecking
 * phase in query compilation.
 */
public class TypeCheckCtx implements NodeProcessorCtx {
  protected static final Logger LOG = LoggerFactory.getLogger(TypeCheckCtx.class);

  /**
   * The row resolver of the previous operator. This field is used to generate
   * expression descriptors from the expression ASTs.
   */
  private RowResolver inputRR;

  private final boolean useCaching;

  /**
   * Receives translations which will need to be applied during unparse.
   */
  private UnparseTranslator unparseTranslator;

  /**
   * Potential typecheck error reason.
   */
  private String error;

  /**
   * The node that generated the potential typecheck error
   */
  private ASTNode errorSrcNode;

  /**
   * Whether to allow stateful UDF invocations.
   */
  private boolean allowStatefulFunctions;

  private boolean allowDistinctFunctions;

  private final boolean allowGBExprElimination;

  private final boolean allowAllColRef;

  private final boolean allowFunctionStar;

  private final boolean allowWindowing;

  // "[]" : LSQUARE/INDEX Expression
  private final boolean allowIndexExpr;

  private final boolean allowSubQueryExpr;

  /**
   * Constructor.
   *
   * @param inputRR
   *          The input row resolver of the previous operator.
   */
  public TypeCheckCtx(RowResolver inputRR) {
    this(inputRR, true);
  }

  public TypeCheckCtx(RowResolver inputRR, boolean useCaching) {
    this(inputRR, useCaching, false, true, true, true, true, true, true, true);
  }

  public TypeCheckCtx(RowResolver inputRR, boolean useCaching, boolean allowStatefulFunctions,
      boolean allowDistinctFunctions, boolean allowGBExprElimination, boolean allowAllColRef,
      boolean allowFunctionStar, boolean allowWindowing,
      boolean allowIndexExpr, boolean allowSubQueryExpr) {
    setInputRR(inputRR);
    error = null;
    this.useCaching = useCaching;
    this.allowStatefulFunctions = allowStatefulFunctions;
    this.allowDistinctFunctions = allowDistinctFunctions;
    this.allowGBExprElimination = allowGBExprElimination;
    this.allowAllColRef = allowAllColRef;
    this.allowFunctionStar = allowFunctionStar;
    this.allowWindowing = allowWindowing;
    this.allowIndexExpr = allowIndexExpr;
    this.allowSubQueryExpr = allowSubQueryExpr;
  }

  /**
   * @param inputRR
   *          the inputRR to set
   */
  public void setInputRR(RowResolver inputRR) {
    this.inputRR = inputRR;
  }

  /**
   * @return the inputRR
   */
  public RowResolver getInputRR() {
    return inputRR;
  }

  /**
   * @param unparseTranslator
   *          the unparseTranslator to set
   */
  public void setUnparseTranslator(UnparseTranslator unparseTranslator) {
    this.unparseTranslator = unparseTranslator;
  }

  /**
   * @return the unparseTranslator
   */
  public UnparseTranslator getUnparseTranslator() {
    return unparseTranslator;
  }

  /**
   * @param allowStatefulFunctions
   *          whether to allow stateful UDF invocations
   */
  public void setAllowStatefulFunctions(boolean allowStatefulFunctions) {
    this.allowStatefulFunctions = allowStatefulFunctions;
  }

  /**
   * @return whether to allow stateful UDF invocations
   */
  public boolean getAllowStatefulFunctions() {
    return allowStatefulFunctions;
  }

  /**
   * @param error
   *          the error to set
   *
   */
  public void setError(String error, ASTNode errorSrcNode) {
    if (LOG.isDebugEnabled()) {
      // Logger the callstack from which the error has been set.
      LOG.debug("Setting error: [" + error + "] from "
          + ((errorSrcNode == null) ? "null" : errorSrcNode.toStringTree()), new Exception());
    }
    this.error = error;
    this.errorSrcNode = errorSrcNode;
  }

  /**
   * @return the error
   */
  public String getError() {
    return error;
  }

  public ASTNode getErrorSrcNode() {
    return errorSrcNode;
  }

  public void setAllowDistinctFunctions(boolean allowDistinctFunctions) {
    this.allowDistinctFunctions = allowDistinctFunctions;
  }

  public boolean getAllowDistinctFunctions() {
    return allowDistinctFunctions;
  }

  public boolean getAllowGBExprElimination() {
    return allowGBExprElimination;
  }

  public boolean getallowAllColRef() {
    return allowAllColRef;
  }

  public boolean getallowFunctionStar() {
    return allowFunctionStar;
  }

  public boolean getallowWindowing() {
    return allowWindowing;
  }

  public boolean getallowIndexExpr() {
    return allowIndexExpr;
  }

  public boolean getallowSubQueryExpr() {
    return allowSubQueryExpr;
  }

  public boolean isUseCaching() {
    return useCaching;
  }
}
