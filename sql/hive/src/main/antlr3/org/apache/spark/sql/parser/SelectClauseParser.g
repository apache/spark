/**
   Licensed to the Apache Software Foundation (ASF) under one or more 
   contributor license agreements.  See the NOTICE file distributed with 
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with 
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
parser grammar SelectClauseParser;

options
{
output=AST;
ASTLabelType=CommonTree;
backtrack=false;
k=3;
}

@members {
  @Override
  public Object recoverFromMismatchedSet(IntStream input,
      RecognitionException re, BitSet follow) throws RecognitionException {
    throw re;
  }
  @Override
  public void displayRecognitionError(String[] tokenNames,
      RecognitionException e) {
    gParent.errors.add(new ParseError(gParent, e, tokenNames));
  }
  protected boolean useSQL11ReservedKeywordsForIdentifier() {
    return gParent.useSQL11ReservedKeywordsForIdentifier();
  }
}

@rulecatch {
catch (RecognitionException e) {
  throw e;
}
}

//----------------------- Rules for parsing selectClause -----------------------------
// select a,b,c ...
selectClause
@init { gParent.pushMsg("select clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_SELECT hintClause? (((KW_ALL | dist=KW_DISTINCT)? selectList)
                          | (transform=KW_TRANSFORM selectTrfmClause))
     -> {$transform == null && $dist == null}? ^(TOK_SELECT hintClause? selectList)
     -> {$transform == null && $dist != null}? ^(TOK_SELECTDI hintClause? selectList)
     -> ^(TOK_SELECT hintClause? ^(TOK_SELEXPR selectTrfmClause) )
    |
    trfmClause  ->^(TOK_SELECT ^(TOK_SELEXPR trfmClause))
    ;

selectList
@init { gParent.pushMsg("select list", state); }
@after { gParent.popMsg(state); }
    :
    selectItem ( COMMA  selectItem )* -> selectItem+
    ;

selectTrfmClause
@init { gParent.pushMsg("transform clause", state); }
@after { gParent.popMsg(state); }
    :
    LPAREN selectExpressionList RPAREN
    inSerde=rowFormat inRec=recordWriter
    KW_USING StringLiteral
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))?
    outSerde=rowFormat outRec=recordReader
    -> ^(TOK_TRANSFORM selectExpressionList $inSerde $inRec StringLiteral $outSerde $outRec aliasList? columnNameTypeList?)
    ;

hintClause
@init { gParent.pushMsg("hint clause", state); }
@after { gParent.popMsg(state); }
    :
    DIVIDE STAR PLUS hintList STAR DIVIDE -> ^(TOK_HINTLIST hintList)
    ;

hintList
@init { gParent.pushMsg("hint list", state); }
@after { gParent.popMsg(state); }
    :
    hintItem (COMMA hintItem)* -> hintItem+
    ;

hintItem
@init { gParent.pushMsg("hint item", state); }
@after { gParent.popMsg(state); }
    :
    hintName (LPAREN hintArgs RPAREN)? -> ^(TOK_HINT hintName hintArgs?)
    ;

hintName
@init { gParent.pushMsg("hint name", state); }
@after { gParent.popMsg(state); }
    :
    KW_MAPJOIN -> TOK_MAPJOIN
    | KW_STREAMTABLE -> TOK_STREAMTABLE
    ;

hintArgs
@init { gParent.pushMsg("hint arguments", state); }
@after { gParent.popMsg(state); }
    :
    hintArgName (COMMA hintArgName)* -> ^(TOK_HINTARGLIST hintArgName+)
    ;

hintArgName
@init { gParent.pushMsg("hint argument name", state); }
@after { gParent.popMsg(state); }
    :
    identifier
    ;

selectItem
@init { gParent.pushMsg("selection target", state); }
@after { gParent.popMsg(state); }
    :
    (tableAllColumns) => tableAllColumns -> ^(TOK_SELEXPR tableAllColumns)
    |
    ( expression
      ((KW_AS? identifier) | (KW_AS LPAREN identifier (COMMA identifier)* RPAREN))?
    ) -> ^(TOK_SELEXPR expression identifier*)
    ;

trfmClause
@init { gParent.pushMsg("transform clause", state); }
@after { gParent.popMsg(state); }
    :
    (   KW_MAP    selectExpressionList
      | KW_REDUCE selectExpressionList )
    inSerde=rowFormat inRec=recordWriter
    KW_USING StringLiteral
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))?
    outSerde=rowFormat outRec=recordReader
    -> ^(TOK_TRANSFORM selectExpressionList $inSerde $inRec StringLiteral $outSerde $outRec aliasList? columnNameTypeList?)
    ;

selectExpression
@init { gParent.pushMsg("select expression", state); }
@after { gParent.popMsg(state); }
    :
    (tableAllColumns) => tableAllColumns
    |
    expression
    ;

selectExpressionList
@init { gParent.pushMsg("select expression list", state); }
@after { gParent.popMsg(state); }
    :
    selectExpression (COMMA selectExpression)* -> ^(TOK_EXPLIST selectExpression+)
    ;

//---------------------- Rules for windowing clauses -------------------------------
window_clause 
@init { gParent.pushMsg("window_clause", state); }
@after { gParent.popMsg(state); } 
:
  KW_WINDOW window_defn (COMMA window_defn)* -> ^(KW_WINDOW window_defn+)
;  

window_defn 
@init { gParent.pushMsg("window_defn", state); }
@after { gParent.popMsg(state); } 
:
  Identifier KW_AS window_specification -> ^(TOK_WINDOWDEF Identifier window_specification)
;  

window_specification 
@init { gParent.pushMsg("window_specification", state); }
@after { gParent.popMsg(state); } 
:
  (Identifier | ( LPAREN Identifier? partitioningSpec? window_frame? RPAREN)) -> ^(TOK_WINDOWSPEC Identifier? partitioningSpec? window_frame?)
;

window_frame :
 window_range_expression |
 window_value_expression
;

window_range_expression 
@init { gParent.pushMsg("window_range_expression", state); }
@after { gParent.popMsg(state); } 
:
 KW_ROWS sb=window_frame_start_boundary -> ^(TOK_WINDOWRANGE $sb) |
 KW_ROWS KW_BETWEEN s=window_frame_boundary KW_AND end=window_frame_boundary -> ^(TOK_WINDOWRANGE $s $end)
;

window_value_expression 
@init { gParent.pushMsg("window_value_expression", state); }
@after { gParent.popMsg(state); } 
:
 KW_RANGE sb=window_frame_start_boundary -> ^(TOK_WINDOWVALUES $sb) |
 KW_RANGE KW_BETWEEN s=window_frame_boundary KW_AND end=window_frame_boundary -> ^(TOK_WINDOWVALUES $s $end)
;

window_frame_start_boundary 
@init { gParent.pushMsg("windowframestartboundary", state); }
@after { gParent.popMsg(state); } 
:
  KW_UNBOUNDED KW_PRECEDING  -> ^(KW_PRECEDING KW_UNBOUNDED) | 
  KW_CURRENT KW_ROW  -> ^(KW_CURRENT) |
  Number KW_PRECEDING -> ^(KW_PRECEDING Number)
;

window_frame_boundary 
@init { gParent.pushMsg("windowframeboundary", state); }
@after { gParent.popMsg(state); } 
:
  KW_UNBOUNDED (r=KW_PRECEDING|r=KW_FOLLOWING)  -> ^($r KW_UNBOUNDED) | 
  KW_CURRENT KW_ROW  -> ^(KW_CURRENT) |
  Number (d=KW_PRECEDING | d=KW_FOLLOWING ) -> ^($d Number)
;   

