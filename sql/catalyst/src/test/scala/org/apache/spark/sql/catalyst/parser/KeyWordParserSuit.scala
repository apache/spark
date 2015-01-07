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


import scala.language.implicitConversions
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.input.CharArrayReader.EofCh
import scala.util.parsing.combinator.lexical._
import org.scalatest.FunSuite

class KeyWordParserSuit extends FunSuite {

    val testDDL = s"""
        |creAtE TEMPORARY TABLE hbase_people
        |USING com.shengli.spark.hbase
        |OPTIONS (
        |  sparksql_table_schema   '(row_key string, name string, age int, job string)',
        |  hbase_table_name    'people',
        |  hbase_table_schema '(:key , profile:name , profile:age , career:job )'
		 |)
        |sERDEPRopERTIES (
		 |  path 'temp_path'
		 |)
		 |TESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTEST(
		 | test 'test_keyword'
		 )""".stripMargin

    val allCaseVersionsParser =  new AllCaseVersionsParser()
    val lowerCaseKeyWordParser = new LowerCaseParser()
    var ret = ""

    test("SPARK-5009 reproduce the stackoverflow exception") {
        try {
            val rs =allCaseVersionsParser(testDDL)
        }
        catch {
            case e: java.lang.StackOverflowError =>
                ret = "stackoverflow"
                println("stackoverflow when keyword using all case versions")
        }
        assert(ret=="stackoverflow")
    }

    test("SPARK-5009 fix the stackoverflow exception with keyword lower case way") {
        try {
            val rs =lowerCaseKeyWordParser(testDDL)
            ret = rs.get
        }
        catch {
            case e: java.lang.StackOverflowError =>
                ret = "stackoverflow"
        }
        assert(ret=="parse success")
    }
}


class AllCaseVersionsParser extends StandardTokenParsers with PackratParsers  {
    def apply(input: String): Option[String] = {
        phrase(ddl)(new lexical.Scanner(input)) match {
            case Success(r, x) => Some(r)
            case x =>
                None
        }
    }
    protected case class Keyword(str: String)

    protected implicit def asParser(k: Keyword): Parser[String] =
        lexical.allCaseVersions(k.str).map(x => x : Parser[String]).reduce(_ | _)

    protected val AS = Keyword("AS")
    protected val CREATE = Keyword("CREATE")
    protected val TEMPORARY = Keyword("TEMPORARY")
    protected val TABLE = Keyword("TABLE")
    protected val USING = Keyword("USING")
    protected val OPTIONS = Keyword("OPTIONS")
    protected val SERDEPROPERTIES = Keyword("SERDEPROPERTIES")
    protected val TESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTEST = Keyword("TESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTEST")

    // Use reflection to find the reserved words defined in this class.
    protected val reservedWords = this.getClass.getMethods.filter(_.getReturnType == classOf[Keyword]).map(_.invoke(this).asInstanceOf[Keyword].str)

    override val lexical = new LowerCaseSqlLexical(reservedWords)

    protected lazy val ddl: Parser[String] = createTable

    /**
     * CREATE FOREIGN TEMPORARY TABLE avroTable
     * USING org.apache.spark.sql.avro
     * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")
     */
    protected lazy val createTable: Parser[String] =
        CREATE ~ TEMPORARY ~ TABLE ~> ident ~ (USING ~> className) ~ (OPTIONS ~> options) ~ (SERDEPROPERTIES~>serde) ~
            (TESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTEST ~> test).? ^^ {
            case tableName ~ provider ~ opts ~ sd ~ tst  =>
                "parse success"
        }

    protected lazy val test: Parser[Map[String, String]] =
        "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

    protected lazy val serde: Parser[Map[String, String]] =
        "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

    protected lazy val options: Parser[Map[String, String]] =
        "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

    protected lazy val className: Parser[String] = repsep(ident, ".") ^^ { case s => s.mkString(".")}

    protected lazy val pair: Parser[(String, String)] = ident ~ stringLit ^^ { case k ~ v => (k,v) }

}




class LowerCaseParser extends StandardTokenParsers with PackratParsers  {
    def apply(input: String): Option[String] = {
        phrase(ddl)(new lexical.Scanner(input)) match {
            case Success(r, x) => Some(r)
            case x =>
                None
        }
    }
    protected case class Keyword(str: String)

    protected implicit def asParser(k: Keyword): Parser[String] = k.str.toLowerCase

    protected val AS = Keyword("AS")
    protected val CREATE = Keyword("CREATE")
    protected val TEMPORARY = Keyword("TEMPORARY")
    protected val TABLE = Keyword("TABLE")
    protected val USING = Keyword("USING")
    protected val OPTIONS = Keyword("OPTIONS")
    protected val SERDEPROPERTIES = Keyword("SERDEPROPERTIES")
    protected val TESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTEST = Keyword("TESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTEST")

    // Use reflection to find the reserved words defined in this class.
    protected val reservedWords = this.getClass.getMethods.filter(_.getReturnType == classOf[Keyword]).map(_.invoke(this).asInstanceOf[Keyword].str)

    override val lexical = new LowerCaseSqlLexical(reservedWords)

    protected lazy val ddl: Parser[String] = createTable

    /**
     * CREATE FOREIGN TEMPORARY TABLE avroTable
     * USING org.apache.spark.sql.avro
     * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")
     */
    protected lazy val createTable: Parser[String] =
        CREATE ~ TEMPORARY ~ TABLE ~> ident ~ (USING ~> className) ~ (OPTIONS ~> options) ~ (SERDEPROPERTIES~>serde) ~
            (TESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTESTTEST ~> test).? ^^ {
            case tableName ~ provider ~ opts ~ sd ~ tst  =>
                "parse success"
        }

    protected lazy val test: Parser[Map[String, String]] =
        "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

    protected lazy val serde: Parser[Map[String, String]] =
        "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

    protected lazy val options: Parser[Map[String, String]] =
        "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

    protected lazy val className: Parser[String] = repsep(ident, ".") ^^ { case s => s.mkString(".")}

    protected lazy val pair: Parser[(String, String)] = ident ~ stringLit ^^ { case k ~ v => (k,v) }

}

/*
 * This class demonstrate the all case versions , if keyword is long,  the allCaseVersions generate a long Stream.
 * In Parser, when called `asParser` method, the reduce(_|_) will  cause stackoverflow exception
 */
class AllCaseVersionsSqlLexical(val keywords: Seq[String]) extends StdLexical {
    case class FloatLit(chars: String) extends Token {
        override def toString = chars
    }

    reserved ++= keywords.flatMap(w => allCaseVersions(w) )

    delimiters += (
        "@", "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")",
        ",", ";", "%", "{", "}", ":", "[", "]", ".", "&", "|", "^", "~"
        )


    override lazy val token: Parser[Token] =
        ( identChar ~ (identChar | digit).* ^^
            { case first ~ rest => processIdent((first :: rest).mkString) }
            | rep1(digit) ~ ('.' ~> digit.*).? ^^ {
            case i ~ None    => NumericLit(i.mkString)
            case i ~ Some(d) => FloatLit(i.mkString + "." + d.mkString)
        }
            | '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^
            { case chars => StringLit(chars mkString "") }
            | '"' ~> chrExcept('"', '\n', EofCh).* <~ '"' ^^
            { case chars => StringLit(chars mkString "") }
            | '`' ~> chrExcept('`', '\n', EofCh).* <~ '`' ^^
            { case chars => Identifier(chars mkString "") }
            | EofCh ^^^ EOF
            | '\'' ~> failure("unclosed string literal")
            | '"' ~> failure("unclosed string literal")
            | delim
            | failure("illegal character")
            )

    override def identChar = letter | elem('_')

    override def whitespace: Parser[Any] =
        ( whitespaceChar
            | '/' ~ '*' ~ comment
            | '/' ~ '/' ~ chrExcept(EofCh, '\n').*
            | '#' ~ chrExcept(EofCh, '\n').*
            | '-' ~ '-' ~ chrExcept(EofCh, '\n').*
            | '/' ~ '*' ~ failure("unclosed comment")
            ).*

    /** Generate all variations of upper and lower case of a given string */
    def allCaseVersions(s: String, prefix: String = ""): Stream[String] = {
        if (s == "") {
            Stream(prefix)
        } else {
            allCaseVersions(s.tail, prefix + s.head.toLower) ++
                allCaseVersions(s.tail, prefix + s.head.toUpper)
        }
    }
}


/*
 * This class demonstrate the lower case keyword matching strategy
 * Will not cause stackoverflow exception and speed up keyword matching
 */
class LowerCaseSqlLexical(val keywords: Seq[String]) extends StdLexical {
    case class FloatLit(chars: String) extends Token {
        override def toString = chars
    }

    reserved ++= keywords.flatMap(w => Stream(w.toLowerCase()) )

    delimiters += (
        "@", "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")",
        ",", ";", "%", "{", "}", ":", "[", "]", ".", "&", "|", "^", "~"
        )


    override lazy val token: Parser[Token] =
        ( identChar ~ (identChar | digit).* ^^
            {
                case first ~ rest =>
                    val rsIdent = processIdent((first :: rest).mkString.toLowerCase())
                    if(rsIdent.getClass.getCanonicalName.contains("StdTokens.Keyword"))
                        Keyword(rsIdent.chars.toLowerCase())
                    else
                        processIdent((first :: rest).mkString)
            }
            | rep1(digit) ~ ('.' ~> digit.*).? ^^ {
            case i ~ None    => NumericLit(i.mkString)
            case i ~ Some(d) => FloatLit(i.mkString + "." + d.mkString)
        }
            | '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^
            { case chars => StringLit(chars mkString "") }
            | '"' ~> chrExcept('"', '\n', EofCh).* <~ '"' ^^
            { case chars => StringLit(chars mkString "") }
            | '`' ~> chrExcept('`', '\n', EofCh).* <~ '`' ^^
            { case chars => Identifier(chars mkString "") }
            | EofCh ^^^ EOF
            | '\'' ~> failure("unclosed string literal")
            | '"' ~> failure("unclosed string literal")
            | delim
            | failure("illegal character")
            )

    override def identChar = letter | elem('_')

    override def whitespace: Parser[Any] =
        ( whitespaceChar
            | '/' ~ '*' ~ comment
            | '/' ~ '/' ~ chrExcept(EofCh, '\n').*
            | '#' ~ chrExcept(EofCh, '\n').*
            | '-' ~ '-' ~ chrExcept(EofCh, '\n').*
            | '/' ~ '*' ~ failure("unclosed comment")
            ).*

    /** Generate all variations of upper and lower case of a given string */
    def allCaseVersions(s: String, prefix: String = ""): Stream[String] = {
        if (s == "") {
            Stream(prefix)
        } else {
            allCaseVersions(s.tail, prefix + s.head.toLower) ++
                allCaseVersions(s.tail, prefix + s.head.toUpper)
        }
    }
}