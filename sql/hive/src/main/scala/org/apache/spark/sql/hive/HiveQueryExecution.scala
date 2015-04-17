package org.apache.spark.sql.hive

import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.sql.{QueryExecution, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SetCommand, ExecutedCommand}
import org.apache.spark.sql.hive.execution.{HiveNativeCommand, DescribeHiveTableCommand}

/** Extends QueryExecution with hive specific features. */
protected[sql] class HiveQueryExecution(hiveContext: HiveContext, logicalPlan: LogicalPlan)
  extends QueryExecution(hiveContext, logicalPlan) {

  /**
   * Returns the result as a hive compatible sequence of strings.  For native commands, the
   * execution is simply passed back to Hive.
   */
  def stringResult(): Seq[String] = executedPlan match {
    case ExecutedCommand(desc: DescribeHiveTableCommand) =>
      // If it is a describe command for a Hive table, we want to have the output format
      // be similar with Hive.
      desc.run(this.hiveContext).map {
        case Row(name: String, dataType: String, comment) =>
          Seq(name, dataType,
            Option(comment.asInstanceOf[String]).getOrElse(""))
            .map(s => String.format(s"%-20s", s))
            .mkString("\t")
      }
    case command: ExecutedCommand =>
      command.executeCollect().map(_(0).toString)

    case other =>
      val result: Seq[Seq[Any]] = other.executeCollect().map(_.toSeq).toSeq
      // We need the types so we can output struct field names
      val types = analyzed.output.map(_.dataType)
      // Reformat to match hive tab delimited output.
      result.map(_.zip(types).map(HiveContext.toHiveString)).map(_.mkString("\t")).toSeq
  }

  override def simpleString: String =
    logical match {
      case _: HiveNativeCommand => "<Native command: executed by Hive>"
      case _: SetCommand => "<SET command: executed by Hive, and noted by SQLContext>"
      case _ => super.simpleString
    }
}
