/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.parser

import org.antlr.v4.runtime.{DefaultErrorStrategy, InputMismatchException, IntStream, NoViableAltException, Parser, ParserRuleContext, RecognitionException, Recognizer, Token}

/**
 * A [[SparkRecognitionException]] extends the [[RecognitionException]] with more information
 * including the error class and parameters for the error message, which align with the interface
 * of [[SparkThrowableHelper]].
 */
class SparkRecognitionException(
    message: String,
    recognizer: Recognizer[_, _],
    input: IntStream,
    ctx: ParserRuleContext,
    val errorClass: Option[String] = None,
    val messageParameters: Map[String, String] = Map.empty)
  extends RecognitionException(message, recognizer, input, ctx) {

  /** Construct from a given [[RecognitionException]], with additional error information. */
  def this(
      recognitionException: RecognitionException,
      errorClass: String,
      messageParameters: Map[String, String]) =
    this(
      recognitionException.getMessage,
      recognitionException.getRecognizer,
      recognitionException.getInputStream,
      recognitionException.getCtx match {
        case p: ParserRuleContext => p
        case _ => null
      },
      Some(errorClass),
      messageParameters)

  /** Construct with pure errorClass and messageParameter information.  */
  def this(errorClass: String, messageParameters: Map[String, String]) =
    this("", null, null, null, Some(errorClass), messageParameters)
}

/**
 * A [[SparkParserErrorStrategy]] extends the [[DefaultErrorStrategy]], that does special handling
 * on errors.
 *
 * The intention of this class is to provide more information of these errors encountered in
 * ANTLR parser to the downstream consumers, to be able to apply the [[SparkThrowable]] error
 * message framework to these exceptions.
 */
class SparkParserErrorStrategy() extends DefaultErrorStrategy {
  private val userWordDict : Map[String, String] = Map("'<EOF>'" -> "end of input")

  /** Get the user-facing display of the error token. */
  override def getTokenErrorDisplay(t: Token): String = {
    val tokenName = super.getTokenErrorDisplay(t)
    userWordDict.getOrElse(tokenName, tokenName)
  }

  override def reportInputMismatch(recognizer: Parser, e: InputMismatchException): Unit = {
    val exceptionWithErrorClass = new SparkRecognitionException(
      e,
      "PARSE_SYNTAX_ERROR",
      messageParameters = Map(
        "error" -> getTokenErrorDisplay(e.getOffendingToken),
        "hint" -> ""))
    recognizer.notifyErrorListeners(e.getOffendingToken, "", exceptionWithErrorClass)
  }

  override def reportNoViableAlternative(recognizer: Parser, e: NoViableAltException): Unit = {
    val exceptionWithErrorClass = new SparkRecognitionException(
      e,
      "PARSE_SYNTAX_ERROR",
      Map("error" -> getTokenErrorDisplay(e.getOffendingToken), "hint" -> ""))
    recognizer.notifyErrorListeners(e.getOffendingToken, "", exceptionWithErrorClass)
  }

  override def reportUnwantedToken(recognizer: Parser): Unit = {
    if (!this.inErrorRecoveryMode(recognizer)) {
      this.beginErrorCondition(recognizer)
      val errorTokenDisplay = getTokenErrorDisplay(recognizer.getCurrentToken)
      val hint = ": extra input " + errorTokenDisplay
      val exceptionWithErrorClass = new SparkRecognitionException(
        "PARSE_SYNTAX_ERROR",
        Map("error" -> errorTokenDisplay, "hint" -> hint))
      recognizer.notifyErrorListeners(recognizer.getCurrentToken, "", exceptionWithErrorClass)
    }
  }

  override def reportMissingToken(recognizer: Parser): Unit = {
    if (!this.inErrorRecoveryMode(recognizer)) {
      this.beginErrorCondition(recognizer)
      val hint = ": missing " + getExpectedTokens(recognizer).toString(recognizer.getVocabulary)
      val exceptionWithErrorClass = new SparkRecognitionException(
        "PARSE_SYNTAX_ERROR",
        Map("error" -> getTokenErrorDisplay(recognizer.getCurrentToken), "hint" -> hint))
      recognizer.notifyErrorListeners(recognizer.getCurrentToken, "", exceptionWithErrorClass)
    }
  }
}
