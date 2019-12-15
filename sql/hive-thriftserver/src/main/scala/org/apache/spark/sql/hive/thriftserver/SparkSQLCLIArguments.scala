package org.apache.spark.sql.hive.thriftserver

import org.apache.spark.internal.Logging

import scala.util.{Failure, Success, Try}

private[hive] case class SparkSQLCLIArguments(args: Array[String]) extends Logging {

  lazy val parsed: Map[String, Seq[String]] = {
    val optionsMap = Map('V' -> "verbose", 'S' -> "silent", 'H' -> "help")

    val optionsArgMap = Map(
      "--file" -> "file",
      "-i" -> "init-file",
      "--database" -> "database",
      "--hiveconf" -> "hiveconf",
      "-e" -> "quoted-query-string",
      "-f" -> "file",
      "--hivevar" -> "hivevar",
      "-d" -> "define",
      "--define" -> "define",
    )

    lazy val showUsage: String = {
      var output = "Usage:\n"
      output += optionsArgMap.map(pair => s"${pair._1} [ARG]\t${pair._2}").mkString("\n")
      output += "\n\nOptions:\n"
      output += optionsMap.map(pair => s"-${pair._1}|--${pair._2}\t${pair._2}").mkString("\n")
      output
    }

    @scala.annotation.tailrec
    def argHelper(
        innerList: List[String],
        accumulator: Map[String, Seq[String]],
        prevKey: Option[String]): Map[String, Seq[String]] = {

      if (innerList.isEmpty) {
        // End of args list.
        accumulator
      } else {
        if (prevKey.isEmpty) {
          if (!innerList.head.startsWith("-")) {
            throw new IllegalArgumentException(
              s"Unknown parameter: ${innerList.head} \n" + showUsage)
          }
          val optionArg = optionsArgMap.get(innerList.head)
          val optionNonArg = optionsMap.values.toSeq.contains(innerList.head.filter(_ != '-'))

          if (optionArg.nonEmpty || optionNonArg) {
            if (optionNonArg) {
              argHelper(
                innerList.tail,
                accumulator + (innerList.head.filter(_ != '-') -> Nil),
                None)
            } else {
              argHelper(innerList.tail, accumulator, optionArg)
            }
          } else {
            if (!innerList.head.toSet.subsetOf(optionsMap.keySet + '-')) {
              throw new IllegalArgumentException(
                s"Unknown parameter: ${innerList.head} \n" + showUsage)
            }
            val newAccumulator = innerList.head.toSet.filter(_ != '-').map(optionsMap(_) -> Nil)
            argHelper(innerList.tail, accumulator ++ newAccumulator, None)
          }
        } else {
          if (prevKey.get.startsWith("-")) {
            throw new IllegalArgumentException(
              s"Expecting value for: $prevKey, received: ${innerList.head}")
          }
          val newAccumulator = accumulator.getOrElse(prevKey.get, Seq()) ++ Seq(innerList.head)
          argHelper(innerList.tail, accumulator + (prevKey.get -> newAccumulator), None)
        }
      }
    }
    argHelper(args.toList, Map(), None)
  }

  def parse(): Unit = {
    Try(parsed) match {
      case Failure(value) =>
        logError(value.getMessage)
        sys.exit(1)
      case Success(value) =>
        logDebug(value.mkString)
      case _ =>
    }
  }

  lazy val getHiveConfigs: Seq[(String, String)] = {
    parsed
      .getOrElse("hiveconf", Nil)
      .map { x =>
        Try {
          val auxConfigs = x.split("=")
          auxConfigs(0) -> auxConfigs(1)
        }
      }
      .filter(_.isSuccess)
      .map(x => x.get)
  }

  def getHiveConf(key: String): Option[String] = {
    parsed
      .getOrElse("hiveconf", Nil)
      .filter(_.split("=")(0) == key)
      .map(_.split("=")(1))
      .headOption
  }

  def getCurrentDatabase: Option[String] = {
    parsed.getOrElse("database", Nil).headOption
  }

  def getInitFile: Option[String] = {
    parsed.getOrElse("init-file", Nil).headOption
  }

  def getFile: Option[String] = {
    parsed.getOrElse("file", Nil).headOption
  }
}
