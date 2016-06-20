package org.apache.spark.sql.schema

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SchemaUtils {

  /**
   *This method will replace NullType to StringType in a input dataFrame schema
   * @param dataFrame
   * @param sqlContext
   * @return
   */
  def replaceNullTypeToStringType(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {
    require(dataFrame != null, "dataFrame cannot be null")
    require(sqlContext != null, "sqlContext cannot be null")

    val schema = getStructType(dataFrame.schema)

    val df = sqlContext.createDataFrame(dataFrame.rdd, schema)

    df
  }

  /**
   *This method will replace NullType to StringType in a input schema
   * @param st
   * @return
   */
  def getStructType(st: StructType): StructType = {
    require(st != null, "StructType cannot be null")

    val fields = st.fields.toList
    var fieldsNew = List[StructField]()
    var i = 0

    fields.foreach{
      e => {
        fieldsNew = fieldsNew ::: List(StructField(e.name, getDataType(e.dataType), e.nullable, e.metadata))
        i = i + 1
      }
    }
    StructType(fieldsNew.toArray)
  }

  /**
   *
   * @param dataType
   * @return
   */
  private  def getDataType(dataType: Any): DataType = dataType match {
    case mt: MapType => {
      MapType(getDataType(mt.keyType), getDataType(mt.valueType), mt.valueContainsNull)
    }
    case at: ArrayType => {
      ArrayType(getDataType(at.elementType), at.containsNull)
    }
    case st: StructType => {
      getStructType(st.asInstanceOf[StructType])
    }
    case elem => {
      elem.asInstanceOf[DataType] match {
        case NullType => StringType
        case elem => elem
      }
    }
  }
}

