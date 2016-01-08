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

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonErrorNode;

public class ASTErrorNode extends ASTNode {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  CommonErrorNode delegate;

  public ASTErrorNode(TokenStream input, Token start, Token stop,
      RecognitionException e){
    delegate = new CommonErrorNode(input,start,stop,e);
  }

  @Override
  public boolean isNil() { return delegate.isNil(); }

  @Override
  public int getType() { return delegate.getType(); }

  @Override
  public String getText() { return delegate.getText(); }
  @Override
  public String toString() { return delegate.toString(); }
}
