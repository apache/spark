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

   This file is an adaptation of Hive's org/apache/hadoop/hive/ql/IdentifiersParser.g grammar.
*/

parser grammar ExpressionParser;

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
    gParent.displayRecognitionError(tokenNames, e);
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

// fun(par1, par2, par3)
function
@init { gParent.pushMsg("function specification", state); }
@after { gParent.popMsg(state); }
    :
    functionName
    LPAREN
      (
        (STAR) => (star=STAR)
        | (dist=KW_DISTINCT)? (selectExpression (COMMA selectExpression)*)?
      )
    RPAREN (KW_OVER ws=window_specification)?
           -> {$star != null}? ^(TOK_FUNCTIONSTAR functionName $ws?)
           -> {$dist == null}? ^(TOK_FUNCTION functionName (selectExpression+)? $ws?)
                            -> ^(TOK_FUNCTIONDI functionName (selectExpression+)? $ws?)
    ;

functionName
@init { gParent.pushMsg("function name", state); }
@after { gParent.popMsg(state); }
    : // Keyword IF is also a function name
    (KW_IF | KW_ARRAY | KW_MAP | KW_STRUCT | KW_UNIONTYPE) => (KW_IF | KW_ARRAY | KW_MAP | KW_STRUCT | KW_UNIONTYPE)
    |
    (functionIdentifier) => functionIdentifier
    |
    {!useSQL11ReservedKeywordsForIdentifier()}? sql11ReservedKeywordsUsedAsCastFunctionName -> Identifier[$sql11ReservedKeywordsUsedAsCastFunctionName.text]
    ;

castExpression
@init { gParent.pushMsg("cast expression", state); }
@after { gParent.popMsg(state); }
    :
    KW_CAST
    LPAREN
          expression
          KW_AS
          primitiveType
    RPAREN -> ^(TOK_FUNCTION primitiveType expression)
    ;

caseExpression
@init { gParent.pushMsg("case expression", state); }
@after { gParent.popMsg(state); }
    :
    KW_CASE expression
    (KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END -> ^(TOK_FUNCTION KW_CASE expression*)
    ;

whenExpression
@init { gParent.pushMsg("case expression", state); }
@after { gParent.popMsg(state); }
    :
    KW_CASE
     ( KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END -> ^(TOK_FUNCTION KW_WHEN expression*)
    ;

constant
@init { gParent.pushMsg("constant", state); }
@after { gParent.popMsg(state); }
    :
    Number
    | dateLiteral
    | timestampLiteral
    | intervalLiteral
    | StringLiteral
    | stringLiteralSequence
    | BigintLiteral
    | SmallintLiteral
    | TinyintLiteral
    | DoubleLiteral
    | booleanValue
    ;

stringLiteralSequence
    :
    StringLiteral StringLiteral+ -> ^(TOK_STRINGLITERALSEQUENCE StringLiteral StringLiteral+)
    ;

dateLiteral
    :
    KW_DATE StringLiteral ->
    {
      // Create DateLiteral token, but with the text of the string value
      // This makes the dateLiteral more consistent with the other type literals.
      adaptor.create(TOK_DATELITERAL, $StringLiteral.text)
    }
    |
    KW_CURRENT_DATE -> ^(TOK_FUNCTION KW_CURRENT_DATE)
    ;

timestampLiteral
    :
    KW_TIMESTAMP StringLiteral ->
    {
      adaptor.create(TOK_TIMESTAMPLITERAL, $StringLiteral.text)
    }
    |
    KW_CURRENT_TIMESTAMP -> ^(TOK_FUNCTION KW_CURRENT_TIMESTAMP)
    ;

intervalLiteral
    :
    (KW_INTERVAL intervalConstant KW_YEAR KW_TO KW_MONTH) => KW_INTERVAL intervalConstant KW_YEAR KW_TO KW_MONTH
      -> ^(TOK_INTERVAL_YEAR_MONTH_LITERAL intervalConstant)
    | (KW_INTERVAL intervalConstant KW_DAY KW_TO KW_SECOND) => KW_INTERVAL intervalConstant KW_DAY KW_TO KW_SECOND
      -> ^(TOK_INTERVAL_DAY_TIME_LITERAL intervalConstant)
    | KW_INTERVAL
      ((intervalConstant KW_YEAR)=> year=intervalConstant KW_YEAR)?
      ((intervalConstant KW_MONTH)=> month=intervalConstant KW_MONTH)?
      ((intervalConstant KW_WEEK)=> week=intervalConstant KW_WEEK)?
      ((intervalConstant KW_DAY)=> day=intervalConstant KW_DAY)?
      ((intervalConstant KW_HOUR)=> hour=intervalConstant KW_HOUR)?
      ((intervalConstant KW_MINUTE)=> minute=intervalConstant KW_MINUTE)?
      ((intervalConstant KW_SECOND)=> second=intervalConstant KW_SECOND)?
      ((intervalConstant KW_MILLISECOND)=> millisecond=intervalConstant KW_MILLISECOND)?
      ((intervalConstant KW_MICROSECOND)=> microsecond=intervalConstant KW_MICROSECOND)?
      -> ^(TOK_INTERVAL
          ^(TOK_INTERVAL_YEAR_LITERAL $year?)
          ^(TOK_INTERVAL_MONTH_LITERAL $month?)
          ^(TOK_INTERVAL_WEEK_LITERAL $week?)
          ^(TOK_INTERVAL_DAY_LITERAL $day?)
          ^(TOK_INTERVAL_HOUR_LITERAL $hour?)
          ^(TOK_INTERVAL_MINUTE_LITERAL $minute?)
          ^(TOK_INTERVAL_SECOND_LITERAL $second?)
          ^(TOK_INTERVAL_MILLISECOND_LITERAL $millisecond?)
          ^(TOK_INTERVAL_MICROSECOND_LITERAL $microsecond?))
    ;

intervalConstant
    :
    sign=(MINUS|PLUS)? value=Number -> {
      adaptor.create(Number, ($sign != null ? $sign.getText() : "") + $value.getText())
    }
    | StringLiteral
    ;

expression
@init { gParent.pushMsg("expression specification", state); }
@after { gParent.popMsg(state); }
    :
    precedenceOrExpression
    ;

atomExpression
    :
    (KW_NULL) => KW_NULL -> TOK_NULL
    | (constant) => constant
    | castExpression
    | caseExpression
    | whenExpression
    | (functionName LPAREN) => function
    | tableOrColumn
    | (LPAREN KW_SELECT) => subQueryExpression
      -> ^(TOK_SUBQUERY_EXPR ^(TOK_SUBQUERY_OP) subQueryExpression)
    | LPAREN! expression RPAREN!
    ;


precedenceFieldExpression
    :
    atomExpression ((LSQUARE^ expression RSQUARE!) | (DOT^ identifier))*
    ;

precedenceUnaryOperator
    :
    PLUS | MINUS | TILDE
    ;

nullCondition
    :
    KW_NULL -> ^(TOK_ISNULL)
    | KW_NOT KW_NULL -> ^(TOK_ISNOTNULL)
    ;

precedenceUnaryPrefixExpression
    :
    (precedenceUnaryOperator+)=> precedenceUnaryOperator^ precedenceUnaryPrefixExpression
    | precedenceFieldExpression
    ;

precedenceUnarySuffixExpression
    :
    (
    (LPAREN precedenceUnaryPrefixExpression RPAREN) => LPAREN precedenceUnaryPrefixExpression (a=KW_IS nullCondition)? RPAREN
    |
    precedenceUnaryPrefixExpression (a=KW_IS nullCondition)?
    )
    -> {$a != null}? ^(TOK_FUNCTION nullCondition precedenceUnaryPrefixExpression)
    -> precedenceUnaryPrefixExpression
    ;


precedenceBitwiseXorOperator
    :
    BITWISEXOR
    ;

precedenceBitwiseXorExpression
    :
    precedenceUnarySuffixExpression (precedenceBitwiseXorOperator^ precedenceUnarySuffixExpression)*
    ;


precedenceStarOperator
    :
    STAR | DIVIDE | MOD | DIV
    ;

precedenceStarExpression
    :
    precedenceBitwiseXorExpression (precedenceStarOperator^ precedenceBitwiseXorExpression)*
    ;


precedencePlusOperator
    :
    PLUS | MINUS
    ;

precedencePlusExpression
    :
    precedenceStarExpression (precedencePlusOperator^ precedenceStarExpression)*
    ;


precedenceAmpersandOperator
    :
    AMPERSAND
    ;

precedenceAmpersandExpression
    :
    precedencePlusExpression (precedenceAmpersandOperator^ precedencePlusExpression)*
    ;


precedenceBitwiseOrOperator
    :
    BITWISEOR
    ;

precedenceBitwiseOrExpression
    :
    precedenceAmpersandExpression (precedenceBitwiseOrOperator^ precedenceAmpersandExpression)*
    ;


// Equal operators supporting NOT prefix
precedenceEqualNegatableOperator
    :
    KW_LIKE | KW_RLIKE | KW_REGEXP
    ;

precedenceEqualOperator
    :
    precedenceEqualNegatableOperator | EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
    ;

subQueryExpression
    :
    LPAREN! selectStatement[true] RPAREN!
    ;

precedenceEqualExpression
    :
    (LPAREN precedenceBitwiseOrExpression COMMA) => precedenceEqualExpressionMutiple
    |
    precedenceEqualExpressionSingle
    ;

precedenceEqualExpressionSingle
    :
    (left=precedenceBitwiseOrExpression -> $left)
    (
       (KW_NOT precedenceEqualNegatableOperator notExpr=precedenceBitwiseOrExpression)
       -> ^(KW_NOT ^(precedenceEqualNegatableOperator $precedenceEqualExpressionSingle $notExpr))
    | (precedenceEqualOperator equalExpr=precedenceBitwiseOrExpression)
       -> ^(precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr)
    | (KW_NOT KW_IN LPAREN KW_SELECT)=>  (KW_NOT KW_IN subQueryExpression)
       -> ^(KW_NOT ^(TOK_SUBQUERY_EXPR ^(TOK_SUBQUERY_OP KW_IN) subQueryExpression $precedenceEqualExpressionSingle))
    | (KW_NOT KW_IN expressions)
       -> ^(KW_NOT ^(TOK_FUNCTION KW_IN $precedenceEqualExpressionSingle expressions))
    | (KW_IN LPAREN KW_SELECT)=>  (KW_IN subQueryExpression)
       -> ^(TOK_SUBQUERY_EXPR ^(TOK_SUBQUERY_OP KW_IN) subQueryExpression $precedenceEqualExpressionSingle)
    | (KW_IN expressions)
       -> ^(TOK_FUNCTION KW_IN $precedenceEqualExpressionSingle expressions)
    | ( KW_NOT KW_BETWEEN (min=precedenceBitwiseOrExpression) KW_AND (max=precedenceBitwiseOrExpression) )
       -> ^(TOK_FUNCTION Identifier["between"] KW_TRUE $left $min $max)
    | ( KW_BETWEEN (min=precedenceBitwiseOrExpression) KW_AND (max=precedenceBitwiseOrExpression) )
       -> ^(TOK_FUNCTION Identifier["between"] KW_FALSE $left $min $max)
    )*
    | (KW_EXISTS LPAREN KW_SELECT)=> (KW_EXISTS subQueryExpression) -> ^(TOK_SUBQUERY_EXPR ^(TOK_SUBQUERY_OP KW_EXISTS) subQueryExpression)
    ;

expressions
    :
    LPAREN expression (COMMA expression)* RPAREN -> expression+
    ;

//we transform the (col0, col1) in ((v00,v01),(v10,v11)) into struct(col0, col1) in (struct(v00,v01),struct(v10,v11))
precedenceEqualExpressionMutiple
    :
    (LPAREN precedenceBitwiseOrExpression (COMMA precedenceBitwiseOrExpression)+ RPAREN -> ^(TOK_FUNCTION Identifier["struct"] precedenceBitwiseOrExpression+))
    ( (KW_IN LPAREN expressionsToStruct (COMMA expressionsToStruct)+ RPAREN)
       -> ^(TOK_FUNCTION KW_IN $precedenceEqualExpressionMutiple expressionsToStruct+)
    | (KW_NOT KW_IN LPAREN expressionsToStruct (COMMA expressionsToStruct)+ RPAREN)
       -> ^(KW_NOT ^(TOK_FUNCTION KW_IN $precedenceEqualExpressionMutiple expressionsToStruct+)))
    ;

expressionsToStruct
    :
    LPAREN expression (COMMA expression)* RPAREN -> ^(TOK_FUNCTION Identifier["struct"] expression+)
    ;

precedenceNotOperator
    :
    KW_NOT
    ;

precedenceNotExpression
    :
    (precedenceNotOperator^)* precedenceEqualExpression
    ;


precedenceAndOperator
    :
    KW_AND
    ;

precedenceAndExpression
    :
    precedenceNotExpression (precedenceAndOperator^ precedenceNotExpression)*
    ;


precedenceOrOperator
    :
    KW_OR
    ;

precedenceOrExpression
    :
    precedenceAndExpression (precedenceOrOperator^ precedenceAndExpression)*
    ;
