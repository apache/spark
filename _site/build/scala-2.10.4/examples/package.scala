
package object examples {
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit lazy val examplesExecutionContext = global
}
