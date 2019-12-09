package org.apache.spark.sql.hive.thriftserver

import scala.util.{Failure, Try}

private[hive] case class SparkSQLCLIArguments(args: Array[String]) {

  lazy val parsed: Map[String, Seq[String]] = {
    val optionsMap = Map(
      'V' -> "verbose",
      'S' -> "silent",
      'H' -> "help"
    )

    val optionsArgMap = Map(
      "--file" -> "file",
      "--database" -> "database",
      "--hiveconf" -> "hiveconf",
      "-e" -> "quoted-query-string",
      "-f" -> "file",
      "--hivevar" -> "hivevar",
      "-d" -> "define",
      "--define" -> "define",
      "--conf" -> "sparkconf"
    )

    lazy val showUsage: String = {
      var output = "Usage:\n"
      output += optionsArgMap.map(pair => s"${pair._1} [ARG]\t${pair._2}").mkString("\n")
      output += "\n\nOptions:\n"
      output += optionsMap.map(pair => s"-${pair._1}|--${pair._2}\t${pair._2}").mkString("\n")
      output
    }

    @scala.annotation.tailrec
    def argHelper(innerList: List[String],
                  accumMap: Map[String, Seq[String]],
                  prevKey: Option[String]
                 ): Map[String, Seq[String]] = {

      if (innerList.isEmpty) {
        // End of args list.
        accumMap
      } else {
        if (prevKey.isEmpty) {
          if (!innerList.head.startsWith("-")) {
            throw new IllegalArgumentException(s"Unknown parameter: ${innerList.head} \n" + showUsage)
          }
          val optionArg = optionsArgMap.get(innerList.head)
          val optionNonArg = optionsMap.values.toSeq.contains(innerList.head.filter(_ != '-'))

          if (optionArg.nonEmpty || optionNonArg) {
            if (optionNonArg) {
              argHelper(innerList.tail, accumMap + (innerList.head.filter(_ != '-') -> Nil), None)
            } else {
              argHelper(innerList.tail, accumMap, optionArg)
            }
          } else {
            if (!innerList.head.toSet.subsetOf(optionsMap.keySet + '-')) {
              throw new IllegalArgumentException(s"Unknown parameter: ${innerList.head} \n" + showUsage)
            }
            val newAccum = innerList.head.toSet.filter(_ != '-').map(optionsMap(_) -> Nil)
            argHelper(innerList.tail, accumMap ++ newAccum, None)
          }
        } else {
          if (prevKey.get.startsWith("-")) {
            throw new IllegalArgumentException(s"Expecting value for: $prevKey, received: ${innerList.head}")
          }
          val newAccum = accumMap.getOrElse(prevKey.get, Seq()) ++ Seq(innerList.head)
          argHelper(innerList.tail, accumMap + (prevKey.get -> newAccum), None)
        }
      }
    }
    argHelper(args.toList, Map(), None)
  }

  def parse(): Unit = {
    Try(parsed) match {
      case Failure(value) =>
        println(value.getMessage)
        System.exit(1)
      case _ =>
    }
  }

  lazy val getSparkConfs: Seq[(String, String)] = {
    parsed.getOrElse("sparkconf", Nil).map {
      x =>
        Try {
          val auxConfs = x.split("=")
          auxConfs(0) -> auxConfs(1)
        }
    }.filter(_.isSuccess).map(x => x.get)
  }


}