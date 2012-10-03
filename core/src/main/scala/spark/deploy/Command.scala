package spark.deploy

import scala.collection.Map

private[spark] case class Command(
    mainClass: String,
    arguments: Seq[String],
    environment: Map[String, String]) {
}
