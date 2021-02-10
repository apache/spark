package fix

import scalafix.v1._
import scala.meta._

class MigrateToSparkSessionBuilder extends SemanticRule("MigrateToSparkSessionBuilder") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val sqlSymbolMatcher = SymbolMatcher.normalized("org.apache.spark.sql.SQLContext")
    val sqlGetOrCreateMatcher = SymbolMatcher.normalized("org.apache.spark.sql.SQLContext.getOrCreate")
    val newCreate = "SparkSession.builder.getOrCreate().sqlContext"
    def matchOnTree(e: Tree): Patch = {
      e match {
        // Rewrite the construction of a SQLContext
        case ns @ Term.New(Init(initArgs)) =>
          initArgs match {
            case (sqlSymbolMatcher(s), _, _) =>
              List(
                Patch.replaceTree(
                  ns,
                  newCreate),
                Patch.addGlobalImport(importer"org.apache.spark.sql.SparkSession")
              ).asPatch
            case _ => Patch.empty
          }
        case ns @ Term.Apply(sqlGetOrCreateMatcher(_), _) =>
          List(
            Patch.replaceTree(
              ns,
              newCreate),
            Patch.addGlobalImport(importer"org.apache.spark.sql.SparkSession")
          ).asPatch
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
