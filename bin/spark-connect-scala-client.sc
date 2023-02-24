import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val conStr = if (sys.env.contains("SPARK_REMOTE")) sys.env("SPARK_REMOTE") else ""
val sessionBuilder = SparkSession.builder()
val spark = if (conStr.isEmpty) sessionBuilder.build() else sessionBuilder.remote(conStr).build()
println(
  """
    |   _____                  __      ______                            __
    |  / ___/____  ____ ______/ /__   / ____/___  ____  ____  ___  _____/ /_
    |  \__ \/ __ \/ __ `/ ___/ //_/  / /   / __ \/ __ \/ __ \/ _ \/ ___/ __/
    | ___/ / /_/ / /_/ / /  / ,<    / /___/ /_/ / / / / / / /  __/ /__/ /_
    |/____/ .___/\__,_/_/  /_/|_|   \____/\____/_/ /_/_/ /_/\___/\___/\__/
    |    /_/
    |""".stripMargin)