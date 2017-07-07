package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, Alias, Literal, NamedExpression, PrettyAttribute}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by gcz on 17-7-6.
  */
object FPGAProjectionSuite {

  val spark = SparkSession.builder().master("local[1]").getOrCreate()
  def main(args: Array[String]) {

    def produceData(): RDD[InternalRow] = {

      val row1 = InternalRow(+20140408, 3L,UTF8String.fromString("2O06141Juc1J1"),UTF8String.fromString("B1102"),UTF8String.fromString("B1101"),1,UTF8String.fromString("HB.WH.02"),UTF8String.fromString("13507190977"),0,+31L,UTF8String.fromString("HB.WH.01.16.06"),+1L,UTF8String.fromString("US10"),UTF8String.fromString("   "),193201716,300,3,0,0,0,0,0,0,0,0,116,23,3,300,0,0,0,+300,+0,+0,+0,20000221,+10)
      val row2 = InternalRow(+20140409, 3L,UTF8String.fromString("2O08141Juc1J2"),UTF8String.fromString("B1101"),UTF8String.fromString("B1101"),1,UTF8String.fromString("HB.WH.03"),UTF8String.fromString("13507190977"),0,+3L,UTF8String.fromString("HB.WH.01.16.06"),+1L,UTF8String.fromString("US10"),UTF8String.fromString("US10"),19320716,3030,3,0,0,0,0,0,0,0,0,1162,3,3,300,0,0,0,+300,+0,+0,+0,20000221,+10)
      val row3 = InternalRow(+20140407, 3L,UTF8String.fromString("2O06141Juc1J6"),UTF8String.fromString("B1101"),UTF8String.fromString("B1101"),1,UTF8String.fromString("HB.WH.01"),UTF8String.fromString("13507190977"),0,+3L,UTF8String.fromString("HB.WH.01.16.06"),+1L,UTF8String.fromString("US10"),UTF8String.fromString("US10"),19320716,300,3,0,0,0,0,0,0,0,0,116,3,3,300,0,0,0,+300,+0,+0,+0,20000221,+10)

      val seqRows: Seq[InternalRow] = Seq(row1)
      spark.sparkContext.parallelize[InternalRow](seqRows)
    }

    val project = new ProjectExec(
      Seq(PrettyAttribute("a", LongType)),
      RangeExec(org.apache.spark.sql.catalyst.plans.logical.Range(1, 1, 1, 1)))

    val data = produceData()
    val back = project.FPGAProjection(data).collect()
    val collectData = data.collect()

    val schema = project.CMCCInputSchema
    // TODO: Use new verification
    back.zip(collectData).foreach { rowPair:(InternalRow, InternalRow) =>
      val (row1, row2) = rowPair
      schema.zipWithIndex.foreach { colTypeWithIndex:(Int, Int) =>
        val (colType, index) = colTypeWithIndex
        if (colType == 1) {
          assert(row1.getInt(index) == row2.getInt(index))
        } else if (colType == 2) {
          assert(row1.getLong(index) == row2.getLong(index))
        } else if (colType == 3) {
          if (row1.getUTF8String(index) == null){
            assert(row2.getUTF8String(index) == null)
          } else {
            assert(row1.getUTF8String(index).equals(row2.getUTF8String(index)))
          }
        }
      }
    }
    println("******************** Right man! ********************")

  }
}
