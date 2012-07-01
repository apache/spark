package spark.deploy

import scala.collection.Map

case class Command(
    mainClass: String,
    arguments: Seq[String],
    environment: Map[String, String]) {
}
