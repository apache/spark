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

import org.antlr.v4.runtime.{DefaultErrorStrategy, InputMismatchException, IntStream, Parser,
  ParserRuleContext, RecognitionException, Recognizer}

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
    val messageParameters: Array[String] = Array.empty)
  extends RecognitionException(message, recognizer, input, ctx) {

  /** Construct from a given [[RecognitionException]], with additional error information. */
  def this(
      recognitionException: RecognitionException,
      errorClass: String,
      messageParameters: Array[String]) =
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
  private def getUserFacingLanguage(input: String) = {
    userWordDict.getOrElse(input, input)
  }

  override def reportInputMismatch(recognizer: Parser, e: InputMismatchException): Unit = {
    // Keep the original error message in ANTLR
    val msg = "mismatched input " +
      this.getTokenErrorDisplay(e.getOffendingToken) +
      " expecting " +
      e.getExpectedTokens.toString(recognizer.getVocabulary)

    val exceptionWithErrorClass = new SparkRecognitionException(
      e,
      "PARSE_INPUT_MISMATCHED",
      Array(getUserFacingLanguage(getTokenErrorDisplay(e.getOffendingToken))))
    recognizer.notifyErrorListeners(e.getOffendingToken, msg, exceptionWithErrorClass)
  }
}
