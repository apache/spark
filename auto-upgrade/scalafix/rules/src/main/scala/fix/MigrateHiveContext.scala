package fix

import scalafix.v1._
import scala.meta._

class MigrateHiveContext extends SemanticRule("MigrateHiveContext") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val hiveSymbolMatcher = SymbolMatcher.normalized("org.apache.spark.sql.hive.HiveContext")
    val hiveGetOrCreateMatcher = SymbolMatcher.normalized("org.apache.spark.sql.hive.HiveContext.getOrCreate")
    val newCreateHive = "SparkSession.builder.enableHiveSupport().getOrCreate().sqlContext"
    val utils = new Utils()
    def matchOnTree(e: Tree): Patch = {
      e match {
        // Rewrite the construction of a HiveContext
        case ns @ Term.New(Init(initArgs)) =>
          initArgs match {
            case (hiveSymbol, _, _) =>
              List(
                Patch.replaceTree(
                  ns,
                  newCreateHive),
                  // TODO Add SparkSession import if missing -- addGlobalImport is broken
                  // Patch.addGlobalImport(importer"org.apache.spark.sql.SparkSession")
                utils.addImportIfNotPresent(importer"org.apache.spark.sql.SparkSession")
              ).asPatch
            case _ => Patch.empty
          }
        case ns @ Term.Apply(hiveGetOrCreateMatcher(_), _) =>
          List(
            Patch.replaceTree(
              ns,
              newCreateHive),
              // TODO Add SparkSession import if missing -- addGlobalImport is broken
              // Patch.addGlobalImport(importer"org.apache.spark.sql.SparkSession")
              utils.addImportIfNotPresent(importer"org.apache.spark.sql.SparkSession")
          ).asPatch

        // HiveContext type name rewrite to SQLContext
        // There should be a way to combine these two rules right?
        // Ideally we could rewrite the import to SqlContext symbol.
        case Import(List(
          Importer(Term.Select(Term.Select(Term.Select(
            Term.Select(Term.Name("org"), Term.Name("apache")),
            Term.Name("spark")),
            Term.Name("sql")),
            Term.Name("hive")),
            List(hiveImports)))) =>
          // Remove HiveContext it's deprecated
          hiveImports.collect {
            case i @ Importee.Name(Name("HiveContext")) =>
              List(
                Patch.removeImportee(i),
                utils.addImportIfNotPresent(importer"org.apache.spark.sql.SQLContext")
                // TODO add SQLContext import if missing -- addGlobalImport is broken
              ).asPatch
            case i @ Importee.Rename(Name("HiveContext"), _) =>
              List(
                Patch.removeImportee(i),
                utils.addImportIfNotPresent(importer"org.apache.spark.sql.SQLContext")
                // TODO add SQLContext import if missing -- addGlobalImport is broken
              ).asPatch
            case _ => Patch.empty
          }.asPatch
        case hiveSymbolMatcher(h) =>
          Patch.replaceTree(h, "SQLContext")
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
