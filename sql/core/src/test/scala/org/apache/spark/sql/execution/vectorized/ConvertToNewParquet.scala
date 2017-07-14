package org.apache.spark.sql.execution.vectorized

import java.nio.ByteBuffer

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._


/**
  * Created by gcz on 17-7-6.
  */
object ConvertToNewParquet {
  def main(args: Array[String]) = {

    val sqlContext = SparkSession.builder().master("spark://localhost:7077").config("spark.executor.memory", "5g").getOrCreate()
    sqlContext.sparkContext.addJar("/home/gcz/Documents/work/spark/sql/core/target/spark-sql_2.11-2.1.1-tests.jar")
    val df = sqlContext.read.parquet("file:///home/gcz/fpga/hubei-cmcc_stripped")


    val toInt = udf((value: Double) => if(value == null) 0 else value.toInt)

    val toLong = udf((value: Double) => if(value == null) 0 else value.toLong)

    val stringToInt = udf((value: String) => if(value == "") 0 else if (value == null) 0 else value.toInt)

    //    sqlContext.udf.register("toInt", (x: Double) => x.toInt)

    val ndf = df.withColumn("TIME_ID", toInt(df("TIME_ID"))).withColumn("MBUSER_ID",  toLong(df("MBUSER_ID"))).withColumn("OPER_TID", df("OPER_TID")).withColumn("NBILLING_TID", df("NBILLING_TID")).withColumn("OBILLING_TID", df("OBILLING_TID")).withColumn("BRAND_ID", toInt(df("BRAND_ID"))).withColumn("AREA_ID",   df("AREA_ID")).withColumn("ACC_NBR", df("ACC_NBR")).withColumn("VPMN_ID",   toInt(df("VPMN_ID"))).withColumn("ACCT_ID", toLong(df("ACCT_ID"))).withColumn("CHANNEL_CODE", df("CHANNEL_CODE")).withColumn("SEX_ID", toLong(df("SEX_ID"))).withColumn("STATE_TID", df("STATE_TID")).withColumn("OSTATE_TID", df("OSTATE_TID")).withColumn("BIRTH_DATE",   stringToInt(df("BIRTH_DATE"))).withColumn("SUM_CHARGE", toInt(df("SUM_CHARGE"))).withColumn("SUM_DURA", toInt(df("SUM_DURA"))).withColumn("SUM_BILL_DURA",   toInt(df("SUM_BILL_DURA"))).withColumn("DURA1",   toInt(df("DURA1"))).withColumn("DURA2", toInt(df("DURA2"))).withColumn("SUM_FLOW", toInt(df("SUM_FLOW"))).withColumn("FLOWUP1",   toInt(df("FLOWUP1"))).withColumn("FLOWUP2",   toInt(df("FLOWUP2"))).withColumn("FLOWDN1", toInt(df("FLOWDN1"))).withColumn("FLOWDN2",   toInt(df("FLOWDN2"))).withColumn("SUM_INFO_LEN",   toInt(df("SUM_INFO_LEN"))).withColumn("SUM_TIMES",   toInt(df("SUM_TIMES"))).withColumn("SUM_BILL_TIMES", toInt(df("SUM_BILL_TIMES"))).withColumn("CHARGE1", toInt(df("CHARGE1"))).withColumn("CHARGE2",   toInt(df("CHARGE2"))).withColumn("CHARGE3",   toInt(df("CHARGE3"))).withColumn("CHARGE4", toInt(df("CHARGE4"))).withColumn("CHARGE1_S",   toInt(df("CHARGE1_S"))).withColumn("CHARGE2_S",   toInt(df("CHARGE2_S"))).withColumn("CHARGE3_S", toInt(df("CHARGE3_S"))).withColumn("CHARGE4_S", toInt(df("CHARGE4_S"))).withColumn("INNET_DATE",   stringToInt(df("INNET_DATE"))).withColumn("TM_TID", toInt(df("TM_TID"))).select("TIME_ID", "MBUSER_ID", "OPER_TID", "NBILLING_TID", "OBILLING_TID", "BRAND_ID", "AREA_ID", "ACC_NBR", "VPMN_ID", "ACCT_ID", "CHANNEL_CODE", "SEX_ID", "STATE_TID", "OSTATE_TID", "BIRTH_DATE", "SUM_CHARGE", "SUM_DURA", "SUM_BILL_DURA", "DURA1", "DURA2", "SUM_FLOW", "FLOWUP1", "FLOWUP2", "FLOWDN1", "FLOWDN2", "SUM_INFO_LEN", "SUM_TIMES", "SUM_BILL_TIMES", "CHARGE1", "CHARGE2", "CHARGE3", "CHARGE4","CHARGE1_S", "CHARGE2_S", "CHARGE3_S", "CHARGE4_S", "INNET_DATE", "TM_TID")
    ndf.write.parquet("file:///home/gcz/fpga/hubei-cmcc_after")

  }
}
