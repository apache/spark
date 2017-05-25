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
package org.apache.spark.ml.feature

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

/**
 * :: Experimental ::
 * Stemmer removes the commoner morphological and inflexional endings from words in English.
 */
@Experimental
@Since("1.7.0")
class Stemmer (override val uid: String)
  extends UnaryTransformer[Seq[String], Seq[String], Stemmer] with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("stemmer"))

  override protected def createTransformFunc: Seq[String] => Seq[String] = {
    terms => terms.map(t => PorterStemmer(t))
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.sameType(ArrayType(StringType)),
      s"Input type must be ArrayType(StringType) but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  override def copy(extra: ParamMap): Stemmer = defaultCopy(extra)
}

@Since("1.7.0")
object Stemmer extends DefaultParamsReadable[Stemmer] {

  @Since("1.7.0")
  override def load(path: String): Stemmer = super.load(path)
}

/**
 * :: Experimental ::
 * Classical Porter stemmer, which is implemented referring to scalanlp/chalk
 * [[https://github.com/scalanlp/chalk/blob/master/src/main/scala/chalk/text/analyze]].
 * The details of PorterStemmer algorithm can be found at
 * [[http://snowball.tartarus.org/algorithms/porter/stemmer.html]].
 */
private[feature] object PorterStemmer {

  def apply(w: String): String = {
    if (w.length < 3) w.toLowerCase
    else {
      val ret = w.toLowerCase.replaceAll("([aeiou])y", "$1Y").replaceAll("^y", "Y")
      step5(step4(step3(step2(step1(ret))))).toLowerCase
    }
  }

  private def step1(w: String): String = step1c(step1b(step1a(w)))

  /** deals with plurals */
  private def step1a(w: String): String = {
    if (w.endsWith("sses") || w.endsWith("ies")) {
      w.substring(0, w.length - 2)
    }
    else if (w.endsWith("s") && w.charAt(w.length - 2) != 's') {
      w.substring(0, w.length - 1)
    }
    else w
  }

  /** deals with  -ed or -ing suffixes */
  private def step1b(w: String): String = {
    def extra(w: String) = {
      if (w.endsWith("at") || w.endsWith("bl") || w.endsWith("iz")) w + 'e'
      else if (doublec(w) && !"lsz".contains(w.last)) w.substring(0, w.length - 1)
      else if (m(w) == 1 && cvc(w)) w + "e"
      else w
    }

    if (w.endsWith("eed")) {
      if (m(w.substring(0, w.length - 3)) > 0) w.substring(0, w.length - 1) else w
    } else if (w.endsWith("ed")) {
      if (w.indexWhere(isVowel) < (w.length - 2)) extra(w.substring(0, w.length - 2))
      else w
    } else if (w.endsWith("ing")) {
      if (w.indexWhere(isVowel) < (w.length - 3)) extra(w.substring(0, w.length - 3))
      else w
    } else w
  }

  /** Turns terminal y to i when there is another vowel in the stem */
  private def step1c(w: String): String = {
    if ((w.last == 'y' || w.last == 'Y') && w.indexWhere(isVowel) < w.length - 1) {
      w.substring(0, w.length - 1) + 'i'
    } else w
  }

  /** Maps double suffixes to single ones */
  private def step2(w: String): String = {
    if (w.length < 3) w
    else {
      val opt = w(w.length - 2) match {
        case 'a' => replaceSuffix(w, "ational", "ate").orElse(replaceSuffix(w, "tional", "tion"))
        case 'c' =>
          replaceSuffix(w, "enci", "ence").orElse(replaceSuffix(w, "anci", "ance"))
        case 'e' => replaceSuffix(w, "izer", "ize")
        case 'g' => replaceSuffix(w, "logi", "log")
        case 'l' => replaceSuffix(w, "bli", "ble")
          .orElse(replaceSuffix(w, "alli", "al"))
          .orElse(replaceSuffix(w, "entli", "ent"))
          .orElse(replaceSuffix(w, "eli", "e"))
          .orElse(replaceSuffix(w, "ousli", "ous"))
        case 'o' => replaceSuffix(w, "ization", "ize")
          .orElse(replaceSuffix(w, "ator", "ate"))
          .orElse(replaceSuffix(w, "ation", "ate"))
        case 's' => replaceSuffix(w, "alism", "al")
          .orElse(replaceSuffix(w, "iveness", "ive"))
          .orElse(replaceSuffix(w, "fulness", "ful"))
          .orElse(replaceSuffix(w, "ousness", "ous"))
        case 't' => replaceSuffix(w, "aliti", "al")
          .orElse(replaceSuffix(w, "iviti", "ive"))
          .orElse(replaceSuffix(w, "biliti", "ble"))
        case _ => None
      }
      opt.filter(w => m(w._1) > 0).map {
        case (a, b) => a + b
      }.getOrElse(w)
    }
  }

  /** Deals with suffixes, -full, -ness etc */
  private def step3(w: String): String = {
    if (w.length < 3) w
    else {
      val opt = w.last match {
        case 'e' => replaceSuffix(w, "icate", "ic")
          .orElse(replaceSuffix(w, "alize", "al"))
          .orElse(replaceSuffix(w, "ative", ""))
        case 'i' => replaceSuffix(w, "iciti", "ic")
        case 'l' => replaceSuffix(w, "ical", "ic").orElse(replaceSuffix(w, "ful", ""))
        case 's' => replaceSuffix(w, "ness", "")
        case _ => None
      }
      opt.filter(w => m(w._1) > 0).map {
        case (a, b) => a + b
      }.getOrElse(w)
    }
  }

  /** Takes off -ant, -ence, etc. */
  private def step4(w: String): String = {
    if (w.length < 3){
      w
    }
    else {
      val opt = w(w.length - 2) match {
        case 'a' => replaceSuffix(w, "al", "")
        case 'c' => replaceSuffix(w, "ance", "").orElse(replaceSuffix(w, "ence", ""))
        case 'e' => replaceSuffix(w, "er", "")
        case 'i' => replaceSuffix(w, "ic", "")
        case 'l' => replaceSuffix(w, "able", "").orElse(replaceSuffix(w, "ible", ""))
        case 'n' => replaceSuffix(w, "ant", "")
          .orElse(replaceSuffix(w, "ement", ""))
          .orElse(replaceSuffix(w, "ment", ""))
          .orElse(replaceSuffix(w, "ent", ""))

        case 'o' => replaceSuffix(w, "ion", "")
          .filter(a => a._1.endsWith("t") || a._1.endsWith("s"))
          .orElse(replaceSuffix(w, "ou", ""))
        case 's' => replaceSuffix(w, "ism", "")
        case 't' => replaceSuffix(w, "ate", "").orElse(replaceSuffix(w, "iti", ""))
        case 'u' => replaceSuffix(w, "ous", "")
        case 'v' => replaceSuffix(w, "ive", "")
        case 'z' => replaceSuffix(w, "ize", "")
        case _ => None
      }
      opt.filter(w => m(w._1) > 1).map {
        case (a, b) => a + b
      }.getOrElse(w)
    }
  }

  /** Removes a final -e */
  private def step5(w: String): String = {
    if (w.length < 3) w else step5b(step5a(w))
  }

  private def step5a(w: String): String = {
    if (w.length < 3) w
    else
    if (w.last == 'e') {
      val n = m(w)
      if (n > 1) w.substring(0, w.length - 1)
      else if (n == 1 && !cvc(w.substring(0, w.length - 1))) w.substring(0, w.length - 1)
      else w
    }
    else {
      w
    }
  }

  private def step5b(w: String): String = {
    if (w.last == 'l' && doublec(w) && m(w) > 1) w.substring(0, w.length - 1)
    else w
  }

  /** number of "consonant sequences" in the current term */
  private def m(w: String): Int = {
    val firstV = w.indexWhere(isVowel)
    if (firstV == -1) 0
    else {
      var m = 0
      var x: Seq[Char] = w.substring(firstV)
      if (x.isEmpty) m
      else {
        while (x.nonEmpty) {
          x = x.dropWhile(isVowel)
          if (x.isEmpty) return m
          m += 1
          if (m > 1) return m // don't need anything bigger than this.
          x = x.dropWhile(isConsonant)
        }
        m
      }
    }
  }

  /** if the term ends with CVC */
  private def cvc(w: String): Boolean = (
    w.length > 2
      && isConsonant(w.last)
      && !("wxY" contains w.last)
      && isVowel(w(w.length - 2))
      && isConsonant(w.charAt(w.length - 3))
    )

  /** if the term ends with double consonent */
  private def doublec(w: String): Boolean = {
    w.length > 2 && w.last == w.charAt(w.length - 2) && isConsonant(w.last)
  }

  private def isConsonant(letter: Char): Boolean = !isVowel(letter)

  private def isVowel(letter: Char): Boolean = "aeiouy" contains letter

  private def replaceSuffix(w: String, suffix: String, repl: String): Option[(String, String)] = {
    if (w endsWith suffix) Some((w.substring(0, w.length - suffix.length), repl))
    else None
  }
}



