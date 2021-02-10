package fix

import scalafix.v1._
import scala.meta._

class MigrateDeprecatedDataFrameReaderFuns extends SemanticRule("MigrateDeprecatedDataFrameReaderFuns") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val readerMatcher = SymbolMatcher.normalized("org.apache.spark.sql.DataFrameReader")
    val jsonReaderMatcher = SymbolMatcher.normalized("org.apache.spark.sql.DataFrameReader.json")
    val utils = new Utils()

    def matchOnTree(e: Tree): Patch = {
      e match {
        case ns @ Term.Apply(jsonReaderMatcher(reader), List(param)) =>
          param match {
            case utils.rddMatcher(rdd) =>
              (Patch.addLeft(rdd, "session.createDataset(") + Patch.addRight(rdd, ")(Encoders.STRING)") +
                utils.addImportIfNotPresent(importer"org.apache.spark.sql.Encoders"))
            case _ =>
              Patch.empty
          }
        case elem @ _ =>
          elem.children match {
            case Nil => Patch.empty
            case _ => elem.children.map(matchOnTree).asPatch
          }
      }
    }
    matchOnTree(doc.tree)
  }
}
