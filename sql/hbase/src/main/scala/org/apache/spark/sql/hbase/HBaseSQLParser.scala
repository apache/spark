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
package org.apache.spark.sql.hbase

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.hbase.execution._
import org.apache.spark.util.Utils

object HBaseSQLParser {
  def getKeywords: Seq[String] = {
    val hbaseSqlFields =
      Class.forName("org.apache.spark.sql.hbase.HBaseSQLParser").getDeclaredFields
    val sparkSqlFields = Class.forName("org.apache.spark.sql.catalyst.SqlParser").getDeclaredFields
    var keywords = hbaseSqlFields.filter(x => x.getName.charAt(0).isUpper).map(_.getName)
    keywords ++= sparkSqlFields.filter(x => x.getName.charAt(0).isUpper).map(_.getName)
    keywords.toSeq
  }
}

class HBaseSQLParser extends SqlParser {

  protected val ADD = Keyword("ADD")
  protected val ALTER = Keyword("ALTER")
  protected val BOOLEAN = Keyword("BOOLEAN")
  protected val BYTE = Keyword("BYTE")
  protected val COLS = Keyword("COLS")
  protected val CREATE = Keyword("CREATE")
  protected val DATA = Keyword("DATA")
  protected val DESCRIBE = Keyword("DESCRIBE")
  protected val DROP = Keyword("DROP")
  protected val EXISTS = Keyword("EXISTS")
  protected val FIELDS = Keyword("FIELDS")
  protected val FLOAT = Keyword("FLOAT")
  protected val INPATH = Keyword("INPATH")
  protected val INT = Keyword("INT")
  protected val INTEGER = Keyword("INTEGER")
  protected val KEY = Keyword("KEY")
  protected val LOAD = Keyword("LOAD")
  protected val LOCAL = Keyword("LOCAL")
  protected val LONG = Keyword("LONG")
  protected val MAPPED = Keyword("MAPPED")
  protected val PRIMARY = Keyword("PRIMARY")
  protected val PARALL = Keyword("PARALL")
  protected val SHORT = Keyword("SHORT")
  protected val SHOW = Keyword("SHOW")
  protected val TABLES = Keyword("TABLES")
  protected val VALUES = Keyword("VALUES")
  protected val TERMINATED = Keyword("TERMINATED")

  override protected lazy val start: Parser[LogicalPlan] =
    (select *
      (UNION ~ ALL ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Union(q1, q2)}
        | INTERSECT ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Intersect(q1, q2)}
        | EXCEPT ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Except(q1, q2)}
        | UNION ~ DISTINCT.? ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Distinct(Union(q1, q2))}
        )
      | insert | create | drop | alterDrop | alterAdd | load | show | describe
      )

  override protected lazy val insert: Parser[LogicalPlan] =
    (INSERT ~> INTO ~> relation ~ select <~ opt(";") ^^ {
      case r ~ s => InsertIntoTable(r, Map[String, Option[String]](), s, overwrite = false)
    }
      |
      INSERT ~> INTO ~> ident ~ (VALUES ~> "(" ~> values <~ ")") ^^ {
        case tableName ~ valueSeq =>
          val valueStringSeq = valueSeq.map { case v =>
            if (v.value == null) null
            else v.value.toString
          }
          InsertValueIntoTableCommand(tableName, valueStringSeq)
      }
      )

  protected lazy val create: Parser[LogicalPlan] =
    CREATE ~> TABLE ~> ident ~
      ("(" ~> tableCols <~ ",") ~
      (PRIMARY ~> KEY ~> "(" ~> keys <~ ")" <~ ")") ~
      (MAPPED ~> BY ~> "(" ~> opt(nameSpace)) ~
      (ident <~ ",") ~
      (COLS ~> "=" ~> "[" ~> expressions <~ "]" <~ ")") <~ opt(";") ^^ {

      case tableName ~ tableColumns ~ keySeq ~ tableNameSpace ~ hbaseTableName ~ mappingInfo =>
        // Since the lexical can not recognize the symbol "=" as we expected, we compose it
        // to expression first and then translate it into Map[String, (String, String)].
        // TODO: Now get the info by hacking, need to change it into normal way if possible
        val infoMap: Map[String, (String, String)] =
          mappingInfo.map { case EqualTo(e1, e2) =>
            val info = e2.toString.substring(1).split('.')
            if (info.length != 2) throw new Exception("\nSyntax Error of Create Table")
            e1.toString.substring(1) ->(info(0), info(1))
          }.toMap


        // Check whether the column info are correct or not
        val tableColSet = tableColumns.unzip._1.toSet
        val keySet = keySeq.toSet
        if (tableColSet.size != tableColumns.length ||
          keySet.size != keySeq.length ||
          !(keySet union infoMap.keySet).equals(tableColSet) ||
          (keySet intersect infoMap.keySet).nonEmpty
        ) {
          throw new Exception(
            "The Column Info of Create Table are not correct")
        }

        val customizedNameSpace = tableNameSpace.getOrElse("")

        val divideTableColsByKeyOrNonkey = tableColumns.partition {
          case (name, _) =>
            keySeq.contains(name)
        }
        val dataTypeOfKeyCols = divideTableColsByKeyOrNonkey._1
        val dataTypeOfNonkeyCols = divideTableColsByKeyOrNonkey._2

        // Get Key Info
        val keyColsWithDataType = keySeq.map {
          key => {
            val typeOfKey = dataTypeOfKeyCols.find(_._1 == key).get._2
            (key, typeOfKey)
          }
        }

        // Get Nonkey Info
        val nonKeyCols = dataTypeOfNonkeyCols.map {
          case (name, typeOfData) =>
            val infoElem = infoMap.get(name).get
            (name, typeOfData, infoElem._1, infoElem._2)
        }

        val colsSeqString = tableColumns.unzip._1.reduceLeft(_ + "," + _)
        val keyColsString = keyColsWithDataType
          .map(k => k._1 + "," + k._2)
          .reduceLeft(_ + ";" + _)
        val nonkeyColsString = if (nonKeyCols.isEmpty) ""
        else {
          nonKeyCols
            .map(k => k._1 + "," + k._2 + "," + k._3 + "," + k._4)
            .reduceLeft(_ + ";" + _)
        }

        val opts: Map[String, String] = Seq(
          ("tableName", tableName),
          ("namespace", customizedNameSpace),
          ("hbaseTableName", hbaseTableName),
          ("colsSeq", colsSeqString),
          ("keyCols", keyColsString),
          ("nonKeyCols", nonkeyColsString)
        ).toMap

        CreateTable(tableName, "org.apache.spark.sql.hbase.HBaseSource", opts)
    }

  private[hbase] case class CreateTable(
                                         tableName: String,
                                         provider: String,
                                         options: Map[String, String]) extends RunnableCommand {
    // create table of persistent metadata
    def run(sqlContext: SQLContext) = {
      val loader = Utils.getContextOrSparkClassLoader
      val clazz: Class[_] = try loader.loadClass(provider) catch {
        case cnf: java.lang.ClassNotFoundException =>
          try loader.loadClass(provider + ".DefaultSource") catch {
            case cnf: java.lang.ClassNotFoundException =>
              sys.error(s"Failed to load class for data source: $provider")
          }
      }
      val dataSource = clazz.newInstance()
        .asInstanceOf[org.apache.spark.sql.sources.RelationProvider]
      dataSource.createRelation(sqlContext, options)
      Seq.empty
    }
  }

  protected lazy val drop: Parser[LogicalPlan] =
    DROP ~> TABLE ~> ident <~ opt(";") ^^ {
      case tableName => DropHbaseTableCommand(tableName)
    }

  protected lazy val alterDrop: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> ident ~
      (DROP ~> ident) <~ opt(";") ^^ {
      case tableName ~ colName => AlterDropColCommand(tableName, colName)
    }

  protected lazy val alterAdd: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> ident ~
      (ADD ~> tableCol) ~
      (MAPPED ~> BY ~> "(" ~> expressions <~ ")") ^^ {
      case tableName ~ tableColumn ~ mappingInfo =>
        // Since the lexical can not recognize the symbol "=" as we expected, we compose it
        // to expression first and then translate it into Map[String, (String, String)]
        // TODO: Now get the info by hacking, need to change it into normal way if possible
        val infoMap: Map[String, (String, String)] =
          mappingInfo.map { case EqualTo(e1, e2) =>
            val info = e2.toString.substring(1).split('.')
            if (info.length != 2) throw new Exception("\nSyntax Error of Create Table")
            e1.toString.substring(1) ->(info(0), info(1))
          }.toMap
        val familyAndQualifier = infoMap(tableColumn._1)

        AlterAddColCommand(tableName, tableColumn._1, tableColumn._2,
          familyAndQualifier._1, familyAndQualifier._2)
    }

  // Load syntax:
  // LOAD DATA [LOCAL] INPATH filePath [OVERWRITE] INTO TABLE tableName [FIELDS TERMINATED BY char]
  protected lazy val load: Parser[LogicalPlan] =
    (LOAD ~> PARALL.?) ~ (DATA ~> LOCAL.?) ~
      (INPATH ~> stringLit) ~
      (opt(OVERWRITE) ~> INTO ~> TABLE ~> ident) ~
      (FIELDS ~> TERMINATED ~> BY ~> stringLit).? <~ opt(";") ^^ {
      case isparall ~ isLocal ~ filePath ~ table ~ delimiter =>
        if (isparall.isDefined) {
          ParallelizedBulkLoadIntoTableCommand(filePath,
            table, isLocal.isDefined, delimiter)
        } else {
          BulkLoadIntoTableCommand(filePath,
            table, isLocal.isDefined, delimiter)
        }
    }

  // syntax:
  // SHOW TABLES
  protected lazy val show: Parser[LogicalPlan] =
    SHOW ~> TABLES <~ opt(";") ^^^ ShowTablesCommand

  protected lazy val describe: Parser[LogicalPlan] =
    (DESCRIBE ~> ident) ^^ {
      case tableName => DescribeTableCommand(tableName)
    }

  protected lazy val tableCol: Parser[(String, String)] =
    ident ~ (STRING | BYTE | SHORT | INT | INTEGER | LONG | FLOAT | DOUBLE | BOOLEAN) ^^ {
      case e1 ~ e2 => (e1, e2)
    }

  protected lazy val nameSpace: Parser[String] = ident <~ "."

  protected lazy val tableCols: Parser[Seq[(String, String)]] = repsep(tableCol, ",")

  protected lazy val keys: Parser[Seq[String]] = repsep(ident, ",")

  protected lazy val values: Parser[Seq[Literal]] = repsep(literal, ",")

  protected lazy val expressions: Parser[Seq[Expression]] = repsep(expression, ",")

}
