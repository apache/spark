package edu.berkeley.cs.amplab.sparkr

import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.{SQLContext, DataFrame, Row, SaveMode}
//import org.apache.spark.sql.hive.{HiveContext, TestHiveContext}

import edu.berkeley.cs.amplab.sparkr.SerDe._

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

object SQLUtils {
  def createSQLContext(jsc: JavaSparkContext): SQLContext = {
    new SQLContext(jsc.sc)
  }

//  def createHiveContext(jsc: JavaSparkContext): HiveContext = {
//    new HiveContext(jsc.sc)
//  }
//
//  def createTestHiveContext(jsc: JavaSparkContext): TestHiveContext = {
//    new TestHiveContext(jsc.sc)
//  }

  def toSeq[T](arr: Array[T]): Seq[T] = {
    arr.toSeq
  }

  def dfToRowRDD(df: DataFrame): JavaRDD[Array[Byte]] = {
    df.map(r => rowToRBytes(r))
  }

  private[this] def rowToRBytes(row: Row): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    SerDe.writeInt(dos, row.length)
    (0 until row.length).map { idx =>
      val obj: Object = row(idx).asInstanceOf[Object]
      SerDe.writeObject(dos, obj)
    }
    bos.toByteArray()
  }

  def dfToCols(df: DataFrame): Array[Array[Byte]] = {
    // localDF is Array[Row]
    val localDF = df.collect()
    val numCols = df.columns.length
    // dfCols is Array[Array[Any]]
    val dfCols = convertRowsToColumns(localDF, numCols)

    dfCols.map { col =>
      colToRBytes(col)
    } 
  }

  def convertRowsToColumns(localDF: Array[Row], numCols: Int): Array[Array[Any]] = {
    (0 until numCols).map { colIdx =>
      localDF.map { row =>
        row(colIdx)
      }
    }.toArray
  }

  def colToRBytes(col: Array[Any]): Array[Byte] = {
    val numRows = col.length
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    
    SerDe.writeInt(dos, numRows)

    col.map { item =>
      val obj: Object = item.asInstanceOf[Object]
      SerDe.writeObject(dos, obj)
    }
    bos.toByteArray()
  }

  def saveMode(mode: String): SaveMode = {
    mode match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "error" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
    }
  }
}
