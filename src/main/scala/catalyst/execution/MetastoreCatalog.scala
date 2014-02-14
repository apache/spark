package catalyst
package execution

import scala.util.parsing.combinator.RegexParsers

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.{FieldSchema, StorageDescriptor, SerDeInfo}
import org.apache.hadoop.hive.metastore.api.{Table => TTable, Partition => TPartition}
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.mapred.InputFormat

import analysis.Catalog
import expressions._
import plans.logical._
import rules._
import catalyst.types._

import scala.collection.JavaConversions._

class HiveMetastoreCatalog(hiveConf: HiveConf) extends Catalog {
  val client = Hive.get(hiveConf)

  def lookupRelation(
      db: Option[String],
      tableName: String,
      alias: Option[String]): BaseRelation = {
    val databaseName = db.getOrElse(SessionState.get.getCurrentDatabase())
    val table = client.getTable(databaseName, tableName)
    val partitions: Seq[Partition] =
      if (table.isPartitioned) {
        client.getPartitions(table)
      } else {
        Nil
      }

    // Since HiveQL is case insensitive for table names we make them all lowercase.
    MetastoreRelation(
      databaseName.toLowerCase,
      tableName.toLowerCase,
      alias)(table.getTTable, partitions.map(part => part.getTPartition))
  }

  /**
   * Creates any tables required for query execution.
   * For example, because of a CREATE TABLE X AS statement.
   */
  object CreateTables extends Rule[LogicalPlan] {
    import HiveMetastoreTypes._

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case InsertIntoCreatedTable(db, tableName, child) =>
        val databaseName = db.getOrElse(SessionState.get.getCurrentDatabase())

        val table = new Table(databaseName, tableName)
        val schema =
          child.output.map(attr => new FieldSchema(attr.name, toMetastoreType(attr.dataType), ""))
        table.setFields(schema)

        val sd = new StorageDescriptor()
        table.getTTable.setSd(sd)
        sd.setCols(schema)

        // TODO: THESE ARE ALL DEFAULTS, WE NEED TO PARSE / UNDERSTAND the output specs.
        sd.setCompressed(false)
        sd.setParameters(Map[String, String]())
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat")
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
        val serDeInfo = new SerDeInfo()
        serDeInfo.setName(tableName)
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
        serDeInfo.setParameters(Map[String, String]())
        sd.setSerdeInfo(serDeInfo)
        client.createTable(table)

        InsertIntoTable(lookupRelation(Some(databaseName), tableName, None), Map.empty, child)
    }
  }
}

object HiveMetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
    "float" ^^^ FloatType |
    "int" ^^^ IntegerType |
    "tinyint" ^^^ ShortType |
    "double" ^^^ DoubleType |
    "bigint" ^^^ LongType |
    "binary" ^^^ BinaryType |
    "boolean" ^^^ BooleanType |
    "decimal" ^^^ DecimalType |
    "varchar\\((\\d+)\\)".r ^^^ StringType

  protected lazy val arrayType: Parser[DataType] =
    "array" ~> "<" ~> dataType <~ ">" ^^ ArrayType

  protected lazy val mapType: Parser[DataType] =
    "map" ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    "[a-zA-Z0-9]*".r ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    "struct" ~> "<" ~> repsep(structField,",") <~ ">" ^^ StructType

  protected lazy val dataType: Parser[DataType] =
    arrayType |
    mapType |
    structType |
    primitiveType

  def toDataType(metastoreType: String): DataType = parseAll(dataType, metastoreType) match {
    case Success(result, _) => result
    case failure: NoSuccess => sys.error(s"Unsupported dataType: $metastoreType")
  }

  def toMetastoreType(dt: DataType): String = dt match {
    case ArrayType(elementType) => s"array<${toMetastoreType(elementType)}>"
    case StructType(fields) =>
      s"struct<${fields.map(f => s"${f.name}:${toMetastoreType(f.dataType)}").mkString(",")}>"
    case MapType(keyType, valueType) =>
      s"map<${toMetastoreType(keyType)},${toMetastoreType(valueType)}>"
    case StringType => "string"
    case FloatType => "float"
    case IntegerType => "int"
    case ShortType =>"tinyint"
    case DoubleType => "double"
    case LongType => "bigint"
    case BinaryType => "binary"
    case BooleanType => "boolean"
    case DecimalType => "decimal"
  }
}

case class MetastoreRelation(databaseName: String, tableName: String, alias: Option[String])
    (val table: TTable, val partitions: Seq[TPartition])
  extends BaseRelation {
  // TODO: Can we use org.apache.hadoop.hive.ql.metadata.Table as the type of table and
  // use org.apache.hadoop.hive.ql.metadata.Partition as the type of elements of partitions.
  // Right now, using org.apache.hadoop.hive.ql.metadata.Table and
  // org.apache.hadoop.hive.ql.metadata.Partition will cause a NotSerializableException
  // which indicates the SerDe we used is not Serializable.

  def hiveQlTable = new Table(table)

  def hiveQlPartitions = partitions.map { p =>
    new Partition(hiveQlTable, p)
  }

  val tableDesc = new TableDesc(
    Class.forName(hiveQlTable.getSerializationLib).asInstanceOf[Class[Deserializer]],
    hiveQlTable.getInputFormatClass,
    // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
    // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
    // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
    // HiveSequenceFileOutputFormat.
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata
  )

   implicit class SchemaAttribute(f: FieldSchema) {
     def toAttribute = AttributeReference(
       f.getName,
       HiveMetastoreTypes.toDataType(f.getType),
       // Since data can be dumped in randomly with no validation, everything is nullable.
       nullable = true
     )(qualifiers = tableName +: alias.toSeq)
   }

  // Must be a stable value since new attributes are born here.
  val partitionKeys = hiveQlTable.getPartitionKeys.map(_.toAttribute)

  /** Non-partitionKey attributes */
  val attributes = table.getSd.getCols.map(_.toAttribute)

  val output = attributes ++ partitionKeys
}
