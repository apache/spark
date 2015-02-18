package edu.berkeley.cs.amplab.sparkr

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.{StructType}

import edu.berkeley.cs.amplab.sparkr.SerDe._

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

object SQLUtils {
  def createSQLContext(sc: SparkContext): SQLContext = {
    new SQLContext(sc)
  }

  def getColNames(df: DataFrame): Array[String] = {
    df.schema.fields.map(_.name)
  }

  def dfToRowRDD(df: DataFrame): JavaRDD[Array[Byte]] = {
    df.map(r => rowToRBytes(r))
  }

  def rowToRBytes(row: Row): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    SerDe.writeInt(dos, row.length)
    (0 until row.length).map { idx =>
      val obj: Object = row(idx).asInstanceOf[Object]
      SerDe.writeObject(dos, obj)
    }
    bos.toByteArray()
  }

  // We convert the DataFrame into an array of RDDs one for each column
  // Each RDD contains a serialized form of the column per partition.
  def dfToColRDD(df: DataFrame): Array[RDD[Array[Byte]]] = {
    val colRDDs = convertRowsToColumns(df)
    val dfOut = colRDDs.map { col =>
      colToRBytes(col)
    }
    dfOut
  }

  def convertRowsToColumns(df: DataFrame): Array[RDD[Any]] = {
    val numCols = df.schema.fields.length
    val colRDDs = (0 until numCols).map { colIdx =>
       df.map { row =>
         row(colIdx)
       }
    }
    colRDDs.toArray
  }

  def colToRBytes(col: RDD[Any]): RDD[Array[Byte]] = {
    col.mapPartitions { iter =>
      val arr = iter.toArray // Array[Any]
      val bos = new ByteArrayOutputStream()
      val dos = new DataOutputStream(bos)
      val numRowsInPartition = arr.length

      SerDe.writeInt(dos, numRowsInPartition)
      arr.map { item =>
        val obj: Object = item.asInstanceOf[Object]
        SerDe.writeObject(dos, obj)
      }
      Iterator.single(bos.toByteArray())
    }
  }
}
