/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.{TestHive, TestHiveSingleton}
import org.apache.spark.sql.test.SQLTestUtils

/**
 * This suite contains a couple of Hive window tests which fail in the typical setup due to tiny
 * numerical differences or due semantic differences between Hive and Spark.
 */
class WindowQuerySuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
/*
  override def beforeAll(): Unit = {
    super.beforeAll()
    sql("DROP TABLE IF EXISTS part")
    sql(
      """
        |CREATE TABLE part(
        |  p_partkey INT,
        |  p_name STRING,
        |  p_mfgr STRING,
        |  p_brand STRING,
        |  p_type STRING,
        |  p_size INT,
        |  p_container STRING,
        |  p_retailprice DOUBLE,
        |  p_comment STRING)
      """.stripMargin)
    val testData1 = TestHive.getHiveFile("data/files/part_tiny.txt").getCanonicalPath
    sql(
      s"""
         |LOAD DATA LOCAL INPATH '$testData1' overwrite into table part
      """.stripMargin)
  }
*/
  override def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS part")
    } finally {
      super.afterAll()
    }
  }

  test("windowing.q -- 15. testExpressions") {
    // Moved because:
    // - Spark uses a different default stddev (sample instead of pop)
    // - Tiny numerical differences in stddev results.
    // - Different StdDev behavior when n=1 (NaN instead of 0)
    checkAnswer(sql(s"""
      |select  p_mfgr,p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |cume_dist() over(distribute by p_mfgr sort by p_name) as cud,
      |percent_rank() over(distribute by p_mfgr sort by p_name) as pr,
      |ntile(3) over(distribute by p_mfgr sort by p_name) as nt,
      |count(p_size) over(distribute by p_mfgr sort by p_name) as ca,
      |avg(p_size) over(distribute by p_mfgr sort by p_name) as avg,
      |stddev(p_size) over(distribute by p_mfgr sort by p_name) as st,
      |first_value(p_size % 5) over(distribute by p_mfgr sort by p_name) as fv,
      |last_value(p_size) over(distribute by p_mfgr sort by p_name) as lv,
      |first_value(p_size) over w1  as fvW1
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
      """.stripMargin),
      // scalastyle:off
      Seq(
        Row("Manufacturer#1", "almond antique burnished rose metallic", 2, 1, 1, 0.3333333333333333, 0.0, 1, 2, 2.0, 0.0, 2, 2, 2),
        Row("Manufacturer#1", "almond antique burnished rose metallic", 2, 1, 1, 0.3333333333333333, 0.0, 1, 2, 2.0, 0.0, 2, 2, 2),
        Row("Manufacturer#1", "almond antique chartreuse lavender yellow", 34, 3, 2, 0.5, 0.4, 2, 3, 12.666666666666666, 18.475208614068027, 2, 34, 2),
        Row("Manufacturer#1", "almond antique salmon chartreuse burlywood", 6, 4, 3, 0.6666666666666666, 0.6, 2, 4, 11.0, 15.448840301675292, 2, 6, 2),
        Row("Manufacturer#1", "almond aquamarine burnished black steel", 28, 5, 4, 0.8333333333333334, 0.8, 3, 5, 14.4, 15.388307249337076, 2, 28, 34),
        Row("Manufacturer#1", "almond aquamarine pink moccasin thistle", 42, 6, 5, 1.0, 1.0, 3, 6, 19.0, 17.787636155487327, 2, 42, 6),
        Row("Manufacturer#2", "almond antique violet chocolate turquoise", 14, 1, 1, 0.2, 0.0, 1, 1, 14.0, Double.NaN, 4, 14, 14),
        Row("Manufacturer#2", "almond antique violet turquoise frosted", 40, 2, 2, 0.4, 0.25, 1, 2, 27.0, 18.384776310850235, 4, 40, 14),
        Row("Manufacturer#2", "almond aquamarine midnight light salmon", 2, 3, 3, 0.6, 0.5, 2, 3, 18.666666666666668, 19.42506971244462, 4, 2, 14),
        Row("Manufacturer#2", "almond aquamarine rose maroon antique", 25, 4, 4, 0.8, 0.75, 2, 4, 20.25, 16.17353805861084, 4, 25, 40),
        Row("Manufacturer#2", "almond aquamarine sandy cyan gainsboro", 18, 5, 5, 1.0, 1.0, 3, 5, 19.8, 14.042791745233567, 4, 18, 2),
        Row("Manufacturer#3", "almond antique chartreuse khaki white", 17, 1, 1, 0.2, 0.0, 1, 1, 17.0,Double.NaN, 2, 17, 17),
        Row("Manufacturer#3", "almond antique forest lavender goldenrod", 14, 2, 2, 0.4, 0.25, 1, 2, 15.5, 2.1213203435596424, 2, 14, 17),
        Row("Manufacturer#3", "almond antique metallic orange dim", 19, 3, 3, 0.6, 0.5, 2, 3, 16.666666666666668, 2.516611478423583, 2, 19, 17),
        Row("Manufacturer#3", "almond antique misty red olive", 1, 4, 4, 0.8, 0.75, 2, 4, 12.75, 8.098353742170895, 2, 1, 14),
        Row("Manufacturer#3", "almond antique olive coral navajo", 45, 5, 5, 1.0, 1.0, 3, 5, 19.2, 16.037456157383566, 2, 45, 19),
        Row("Manufacturer#4", "almond antique gainsboro frosted violet", 10, 1, 1, 0.2, 0.0, 1, 1, 10.0, Double.NaN, 0, 10, 10),
        Row("Manufacturer#4", "almond antique violet mint lemon", 39, 2, 2, 0.4, 0.25, 1, 2, 24.5, 20.506096654409877, 0, 39, 10),
        Row("Manufacturer#4", "almond aquamarine floral ivory bisque", 27, 3, 3, 0.6, 0.5, 2, 3, 25.333333333333332, 14.571661996262929, 0, 27, 10),
        Row("Manufacturer#4", "almond aquamarine yellow dodger mint", 7, 4, 4, 0.8, 0.75, 2, 4, 20.75, 15.01943185787443, 0, 7, 39),
        Row("Manufacturer#4", "almond azure aquamarine papaya violet", 12, 5, 5, 1.0, 1.0, 3, 5, 19.0, 13.583077707206124, 0, 12, 27),
        Row("Manufacturer#5", "almond antique blue firebrick mint", 31, 1, 1, 0.2, 0.0, 1, 1, 31.0, Double.NaN, 1, 31, 31),
        Row("Manufacturer#5", "almond antique medium spring khaki", 6, 2, 2, 0.4, 0.25, 1, 2, 18.5, 17.67766952966369, 1, 6, 31),
        Row("Manufacturer#5", "almond antique sky peru orange", 2, 3, 3, 0.6, 0.5, 2, 3, 13.0, 15.716233645501712, 1, 2, 31),
        Row("Manufacturer#5", "almond aquamarine dodger light gainsboro", 46, 4, 4, 0.8, 0.75, 2, 4, 21.25, 20.902551678363736, 1, 46, 6),
        Row("Manufacturer#5", "almond azure blanched chiffon midnight", 23, 5, 5, 1.0, 1.0, 3, 5, 21.6, 18.1190507477627, 1, 23, 2)))
      // scalastyle:on
  }

  test("windowing.q -- 20. testSTATs") {
    // Moved because:
    // - Spark uses a different default stddev/variance (sample instead of pop)
    // - Tiny numerical differences in aggregation results.
    checkAnswer(sql("""
      |select p_mfgr,p_name, p_size, sdev, sdev_pop, uniq_data, var, cor, covarp
      |from (
      |select  p_mfgr,p_name, p_size,
      |stddev_pop(p_retailprice) over w1 as sdev,
      |stddev_pop(p_retailprice) over w1 as sdev_pop,
      |collect_set(p_size) over w1 as uniq_size,
      |var_pop(p_retailprice) over w1 as var,
      |corr(p_size, p_retailprice) over w1 as cor,
      |covar_pop(p_size, p_retailprice) over w1 as covarp
      |from part
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name
      |             rows between 2 preceding and 2 following)
      |) t lateral view explode(uniq_size) d as uniq_data
      |order by p_mfgr,p_name, p_size, sdev, sdev_pop, uniq_data, var, cor, covarp
      """.stripMargin),
      // scalastyle:off
      Seq(
        Row("Manufacturer#1", "almond antique burnished rose metallic", 2, 258.10677784349247, 258.10677784349247, 2, 66619.10876874997, 0.811328754177887, 2801.7074999999995),
        Row("Manufacturer#1", "almond antique burnished rose metallic", 2, 258.10677784349247, 258.10677784349247, 6, 66619.10876874997, 0.811328754177887, 2801.7074999999995),
        Row("Manufacturer#1", "almond antique burnished rose metallic", 2, 258.10677784349247, 258.10677784349247, 34, 66619.10876874997, 0.811328754177887, 2801.7074999999995),
        Row("Manufacturer#1", "almond antique burnished rose metallic", 2, 273.70217881648085, 273.70217881648085, 2, 74912.88268888886, 1.0, 4128.782222222221),
        Row("Manufacturer#1", "almond antique burnished rose metallic", 2, 273.70217881648085, 273.70217881648085, 34, 74912.88268888886, 1.0, 4128.782222222221),
        Row("Manufacturer#1", "almond antique chartreuse lavender yellow", 34, 230.9015158547037, 230.9015158547037, 2, 53315.510023999974, 0.6956393773976641, 2210.7864),
        Row("Manufacturer#1", "almond antique chartreuse lavender yellow", 34, 230.9015158547037, 230.9015158547037, 6, 53315.510023999974, 0.6956393773976641, 2210.7864),
        Row("Manufacturer#1", "almond antique chartreuse lavender yellow", 34, 230.9015158547037, 230.9015158547037, 28, 53315.510023999974, 0.6956393773976641, 2210.7864),
        Row("Manufacturer#1", "almond antique chartreuse lavender yellow", 34, 230.9015158547037, 230.9015158547037, 34, 53315.510023999974, 0.6956393773976641, 2210.7864),
        Row("Manufacturer#1", "almond antique salmon chartreuse burlywood", 6, 202.73109328368943, 202.73109328368943, 2, 41099.89618399999, 0.6307859771012139, 2009.9536000000007),
        Row("Manufacturer#1", "almond antique salmon chartreuse burlywood", 6, 202.73109328368943, 202.73109328368943, 6, 41099.89618399999, 0.6307859771012139, 2009.9536000000007),
        Row("Manufacturer#1", "almond antique salmon chartreuse burlywood", 6, 202.73109328368943, 202.73109328368943, 28, 41099.89618399999, 0.6307859771012139, 2009.9536000000007),
        Row("Manufacturer#1", "almond antique salmon chartreuse burlywood", 6, 202.73109328368943, 202.73109328368943, 34, 41099.89618399999, 0.6307859771012139, 2009.9536000000007),
        Row("Manufacturer#1", "almond antique salmon chartreuse burlywood", 6, 202.73109328368943, 202.73109328368943, 42, 41099.89618399999, 0.6307859771012139, 2009.9536000000007),
        Row("Manufacturer#1", "almond aquamarine burnished black steel", 28, 121.60645179738611, 121.60645179738611, 6, 14788.129118749992, 0.2036684720435979, 331.1337500000004),
        Row("Manufacturer#1", "almond aquamarine burnished black steel", 28, 121.60645179738611, 121.60645179738611, 28, 14788.129118749992, 0.2036684720435979, 331.1337500000004),
        Row("Manufacturer#1", "almond aquamarine burnished black steel", 28, 121.60645179738611, 121.60645179738611, 34, 14788.129118749992, 0.2036684720435979, 331.1337500000004),
        Row("Manufacturer#1", "almond aquamarine burnished black steel", 28, 121.60645179738611, 121.60645179738611, 42, 14788.129118749992, 0.2036684720435979, 331.1337500000004),
        Row("Manufacturer#1", "almond aquamarine pink moccasin thistle", 42, 96.57515864168516, 96.57515864168516, 6, 9326.761266666656, -1.4442181184933883E-4, -0.20666666666708502),
        Row("Manufacturer#1", "almond aquamarine pink moccasin thistle", 42, 96.57515864168516, 96.57515864168516, 28, 9326.761266666656, -1.4442181184933883E-4, -0.20666666666708502),
        Row("Manufacturer#1", "almond aquamarine pink moccasin thistle", 42, 96.57515864168516, 96.57515864168516, 42, 9326.761266666656, -1.4442181184933883E-4, -0.20666666666708502),
        Row("Manufacturer#2", "almond antique violet chocolate turquoise", 14, 142.23631697518977, 142.23631697518977, 2, 20231.16986666666, -0.4936952655452319, -1113.7466666666658),
        Row("Manufacturer#2", "almond antique violet chocolate turquoise", 14, 142.23631697518977, 142.23631697518977, 14, 20231.16986666666, -0.4936952655452319, -1113.7466666666658),
        Row("Manufacturer#2", "almond antique violet chocolate turquoise", 14, 142.23631697518977, 142.23631697518977, 40, 20231.16986666666, -0.4936952655452319, -1113.7466666666658),
        Row("Manufacturer#2", "almond antique violet turquoise frosted", 40, 137.7630649884068, 137.7630649884068, 2, 18978.662074999997, -0.5205630897335946, -1004.4812499999995),
        Row("Manufacturer#2", "almond antique violet turquoise frosted", 40, 137.7630649884068, 137.7630649884068, 14, 18978.662074999997, -0.5205630897335946, -1004.4812499999995),
        Row("Manufacturer#2", "almond antique violet turquoise frosted", 40, 137.7630649884068, 137.7630649884068, 25, 18978.662074999997, -0.5205630897335946, -1004.4812499999995),
        Row("Manufacturer#2", "almond antique violet turquoise frosted", 40, 137.7630649884068, 137.7630649884068, 40, 18978.662074999997, -0.5205630897335946, -1004.4812499999995),
        Row("Manufacturer#2", "almond aquamarine midnight light salmon", 2, 130.03972279269132, 130.03972279269132, 2, 16910.329504000005, -0.46908967495720255, -766.1791999999995),
        Row("Manufacturer#2", "almond aquamarine midnight light salmon", 2, 130.03972279269132, 130.03972279269132, 14, 16910.329504000005, -0.46908967495720255, -766.1791999999995),
        Row("Manufacturer#2", "almond aquamarine midnight light salmon", 2, 130.03972279269132, 130.03972279269132, 18, 16910.329504000005, -0.46908967495720255, -766.1791999999995),
        Row("Manufacturer#2", "almond aquamarine midnight light salmon", 2, 130.03972279269132, 130.03972279269132, 25, 16910.329504000005, -0.46908967495720255, -766.1791999999995),
        Row("Manufacturer#2", "almond aquamarine midnight light salmon", 2, 130.03972279269132, 130.03972279269132, 40, 16910.329504000005, -0.46908967495720255, -766.1791999999995),
        Row("Manufacturer#2", "almond aquamarine rose maroon antique", 25, 135.55100986344593, 135.55100986344593, 2, 18374.076275000018, -0.6091405874714462, -1128.1787499999987),
        Row("Manufacturer#2", "almond aquamarine rose maroon antique", 25, 135.55100986344593, 135.55100986344593, 18, 18374.076275000018, -0.6091405874714462, -1128.1787499999987),
        Row("Manufacturer#2", "almond aquamarine rose maroon antique", 25, 135.55100986344593, 135.55100986344593, 25, 18374.076275000018, -0.6091405874714462, -1128.1787499999987),
        Row("Manufacturer#2", "almond aquamarine rose maroon antique", 25, 135.55100986344593, 135.55100986344593, 40, 18374.076275000018, -0.6091405874714462, -1128.1787499999987),
        Row("Manufacturer#2", "almond aquamarine sandy cyan gainsboro", 18, 156.44019460768035, 156.44019460768035, 2, 24473.534488888898, -0.9571686373491605, -1441.4466666666676),
        Row("Manufacturer#2", "almond aquamarine sandy cyan gainsboro", 18, 156.44019460768035, 156.44019460768035, 18, 24473.534488888898, -0.9571686373491605, -1441.4466666666676),
        Row("Manufacturer#2", "almond aquamarine sandy cyan gainsboro", 18, 156.44019460768035, 156.44019460768035, 25, 24473.534488888898, -0.9571686373491605, -1441.4466666666676),
        Row("Manufacturer#3", "almond antique chartreuse khaki white", 17, 196.77422668858057, 196.77422668858057, 14, 38720.0962888889, 0.5557168646224995, 224.6944444444446),
        Row("Manufacturer#3", "almond antique chartreuse khaki white", 17, 196.77422668858057, 196.77422668858057, 17, 38720.0962888889, 0.5557168646224995, 224.6944444444446),
        Row("Manufacturer#3", "almond antique chartreuse khaki white", 17, 196.77422668858057, 196.77422668858057, 19, 38720.0962888889, 0.5557168646224995, 224.6944444444446),
        Row("Manufacturer#3", "almond antique forest lavender goldenrod", 14, 275.1414418985261, 275.1414418985261, 1, 75702.81305000003, -0.6720833036576083, -1296.9000000000003),
        Row("Manufacturer#3", "almond antique forest lavender goldenrod", 14, 275.1414418985261, 275.1414418985261, 14, 75702.81305000003, -0.6720833036576083, -1296.9000000000003),
        Row("Manufacturer#3", "almond antique forest lavender goldenrod", 14, 275.1414418985261, 275.1414418985261, 17, 75702.81305000003, -0.6720833036576083, -1296.9000000000003),
        Row("Manufacturer#3", "almond antique forest lavender goldenrod", 14, 275.1414418985261, 275.1414418985261, 19, 75702.81305000003, -0.6720833036576083, -1296.9000000000003),
        Row("Manufacturer#3", "almond antique metallic orange dim", 19, 260.23473614412046, 260.23473614412046, 1, 67722.11789600001, -0.5703526513979519, -2129.0664),
        Row("Manufacturer#3", "almond antique metallic orange dim", 19, 260.23473614412046, 260.23473614412046, 14, 67722.11789600001, -0.5703526513979519, -2129.0664),
        Row("Manufacturer#3", "almond antique metallic orange dim", 19, 260.23473614412046, 260.23473614412046, 17, 67722.11789600001, -0.5703526513979519, -2129.0664),
        Row("Manufacturer#3", "almond antique metallic orange dim", 19, 260.23473614412046, 260.23473614412046, 19, 67722.11789600001, -0.5703526513979519, -2129.0664),
        Row("Manufacturer#3", "almond antique metallic orange dim", 19, 260.23473614412046, 260.23473614412046, 45, 67722.11789600001, -0.5703526513979519, -2129.0664),
        Row("Manufacturer#3", "almond antique misty red olive", 1, 275.913996235693, 275.913996235693, 1, 76128.53331875002, -0.5774768996448021, -2547.7868749999993),
        Row("Manufacturer#3", "almond antique misty red olive", 1, 275.913996235693, 275.913996235693, 14, 76128.53331875002, -0.5774768996448021, -2547.7868749999993),
        Row("Manufacturer#3", "almond antique misty red olive", 1, 275.913996235693, 275.913996235693, 19, 76128.53331875002, -0.5774768996448021, -2547.7868749999993),
        Row("Manufacturer#3", "almond antique misty red olive", 1, 275.913996235693, 275.913996235693, 45, 76128.53331875002, -0.5774768996448021, -2547.7868749999993),
        Row("Manufacturer#3", "almond antique olive coral navajo", 45, 260.58159187137954, 260.58159187137954, 1, 67902.7660222222, -0.8710736366736884, -4099.731111111111),
        Row("Manufacturer#3", "almond antique olive coral navajo", 45, 260.58159187137954, 260.58159187137954, 19, 67902.7660222222, -0.8710736366736884, -4099.731111111111),
        Row("Manufacturer#3", "almond antique olive coral navajo", 45, 260.58159187137954, 260.58159187137954, 45, 67902.7660222222, -0.8710736366736884, -4099.731111111111),
        Row("Manufacturer#4", "almond antique gainsboro frosted violet", 10, 170.1301188959661, 170.1301188959661, 10, 28944.25735555556, -0.6656975320098423, -1347.4777777777779),
        Row("Manufacturer#4", "almond antique gainsboro frosted violet", 10, 170.1301188959661, 170.1301188959661, 27, 28944.25735555556, -0.6656975320098423, -1347.4777777777779),
        Row("Manufacturer#4", "almond antique gainsboro frosted violet", 10, 170.1301188959661, 170.1301188959661, 39, 28944.25735555556, -0.6656975320098423, -1347.4777777777779),
        Row("Manufacturer#4", "almond antique violet mint lemon", 39, 242.26834609323197, 242.26834609323197, 7, 58693.95151875002, -0.8051852719193339, -2537.328125),
        Row("Manufacturer#4", "almond antique violet mint lemon", 39, 242.26834609323197, 242.26834609323197, 10, 58693.95151875002, -0.8051852719193339, -2537.328125),
        Row("Manufacturer#4", "almond antique violet mint lemon", 39, 242.26834609323197, 242.26834609323197, 27, 58693.95151875002, -0.8051852719193339, -2537.328125),
        Row("Manufacturer#4", "almond antique violet mint lemon", 39, 242.26834609323197, 242.26834609323197, 39, 58693.95151875002, -0.8051852719193339, -2537.328125),
        Row("Manufacturer#4", "almond aquamarine floral ivory bisque", 27, 234.10001662537323, 234.10001662537323, 7, 54802.81778400003, -0.6046935574240581, -1719.8079999999995),
        Row("Manufacturer#4", "almond aquamarine floral ivory bisque", 27, 234.10001662537323, 234.10001662537323, 10, 54802.81778400003, -0.6046935574240581, -1719.8079999999995),
        Row("Manufacturer#4", "almond aquamarine floral ivory bisque", 27, 234.10001662537323, 234.10001662537323, 12, 54802.81778400003, -0.6046935574240581, -1719.8079999999995),
        Row("Manufacturer#4", "almond aquamarine floral ivory bisque", 27, 234.10001662537323, 234.10001662537323, 27, 54802.81778400003, -0.6046935574240581, -1719.8079999999995),
        Row("Manufacturer#4", "almond aquamarine floral ivory bisque", 27, 234.10001662537323, 234.10001662537323, 39, 54802.81778400003, -0.6046935574240581, -1719.8079999999995),
        Row("Manufacturer#4", "almond aquamarine yellow dodger mint", 7, 247.33427141977316, 247.33427141977316, 7, 61174.241818750015, -0.5508665654707869, -1719.0368749999975),
        Row("Manufacturer#4", "almond aquamarine yellow dodger mint", 7, 247.33427141977316, 247.33427141977316, 12, 61174.241818750015, -0.5508665654707869, -1719.0368749999975),
        Row("Manufacturer#4", "almond aquamarine yellow dodger mint", 7, 247.33427141977316, 247.33427141977316, 27, 61174.241818750015, -0.5508665654707869, -1719.0368749999975),
        Row("Manufacturer#4", "almond aquamarine yellow dodger mint", 7, 247.33427141977316, 247.33427141977316, 39, 61174.241818750015, -0.5508665654707869, -1719.0368749999975),
        Row("Manufacturer#4", "almond azure aquamarine papaya violet", 12, 283.33443305668936, 283.33443305668936, 7, 80278.4009555556, -0.7755740084632333, -1867.4888888888881),
        Row("Manufacturer#4", "almond azure aquamarine papaya violet", 12, 283.33443305668936, 283.33443305668936, 12, 80278.4009555556, -0.7755740084632333, -1867.4888888888881),
        Row("Manufacturer#4", "almond azure aquamarine papaya violet", 12, 283.33443305668936, 283.33443305668936, 27, 80278.4009555556, -0.7755740084632333, -1867.4888888888881),
        Row("Manufacturer#5", "almond antique blue firebrick mint", 31, 83.69879024746344, 83.69879024746344, 2, 7005.487488888881, 0.3900430308728505, 418.9233333333353),
        Row("Manufacturer#5", "almond antique blue firebrick mint", 31, 83.69879024746344, 83.69879024746344, 6, 7005.487488888881, 0.3900430308728505, 418.9233333333353),
        Row("Manufacturer#5", "almond antique blue firebrick mint", 31, 83.69879024746344, 83.69879024746344, 31, 7005.487488888881, 0.3900430308728505, 418.9233333333353),
        Row("Manufacturer#5", "almond antique medium spring khaki", 6, 316.68049612345885, 316.68049612345885, 2, 100286.53662500005, -0.7136129117761831, -4090.853749999999),
        Row("Manufacturer#5", "almond antique medium spring khaki", 6, 316.68049612345885, 316.68049612345885, 6, 100286.53662500005, -0.7136129117761831, -4090.853749999999),
        Row("Manufacturer#5", "almond antique medium spring khaki", 6, 316.68049612345885, 316.68049612345885, 31, 100286.53662500005, -0.7136129117761831, -4090.853749999999),
        Row("Manufacturer#5", "almond antique medium spring khaki", 6, 316.68049612345885, 316.68049612345885, 46, 100286.53662500005, -0.7136129117761831, -4090.853749999999),
        Row("Manufacturer#5", "almond antique sky peru orange", 2, 285.4050629824216, 285.4050629824216, 2, 81456.04997600004, -0.712858514567818, -3297.2011999999986),
        Row("Manufacturer#5", "almond antique sky peru orange", 2, 285.4050629824216, 285.4050629824216, 6, 81456.04997600004, -0.712858514567818, -3297.2011999999986),
        Row("Manufacturer#5", "almond antique sky peru orange", 2, 285.4050629824216, 285.4050629824216, 23, 81456.04997600004, -0.712858514567818, -3297.2011999999986),
        Row("Manufacturer#5", "almond antique sky peru orange", 2, 285.4050629824216, 285.4050629824216, 31, 81456.04997600004, -0.712858514567818, -3297.2011999999986),
        Row("Manufacturer#5", "almond antique sky peru orange", 2, 285.4050629824216, 285.4050629824216, 46, 81456.04997600004, -0.712858514567818, -3297.2011999999986),
        Row("Manufacturer#5", "almond aquamarine dodger light gainsboro", 46, 285.43749038756283, 285.43749038756283, 2, 81474.56091875004, -0.9841287871533909, -4871.028125000002),
        Row("Manufacturer#5", "almond aquamarine dodger light gainsboro", 46, 285.43749038756283, 285.43749038756283, 6, 81474.56091875004, -0.9841287871533909, -4871.028125000002),
        Row("Manufacturer#5", "almond aquamarine dodger light gainsboro", 46, 285.43749038756283, 285.43749038756283, 23, 81474.56091875004, -0.9841287871533909, -4871.028125000002),
        Row("Manufacturer#5", "almond aquamarine dodger light gainsboro", 46, 285.43749038756283, 285.43749038756283, 46, 81474.56091875004, -0.9841287871533909, -4871.028125000002),
        Row("Manufacturer#5", "almond azure blanched chiffon midnight", 23, 315.9225931564038, 315.9225931564038, 2, 99807.08486666666, -0.9978877469246935, -5664.856666666666),
        Row("Manufacturer#5", "almond azure blanched chiffon midnight", 23, 315.9225931564038, 315.9225931564038, 23, 99807.08486666666, -0.9978877469246935, -5664.856666666666),
        Row("Manufacturer#5", "almond azure blanched chiffon midnight", 23, 315.9225931564038, 315.9225931564038, 46, 99807.08486666666, -0.9978877469246935, -5664.856666666666)))
      // scalastyle:on
  }

  test("null arguments") {
    checkAnswer(sql("""
        |select  p_mfgr, p_name, p_size,
        |sum(null) over(distribute by p_mfgr sort by p_name) as sum,
        |avg(null) over(distribute by p_mfgr sort by p_name) as avg
        |from part
      """.stripMargin),
      sql("""
        |select  p_mfgr, p_name, p_size,
        |null as sum,
        |null as avg
        |from part
        """.stripMargin))
  }

  test("Exclude clause"){
    withTable("table1", "table2"){
      sql("create table table1 (col1 int, col2 int, col3 int)")
      sql("insert into table1 select 6, 12, 10")
      sql("insert into table1 select 6, 11, 4")
      sql("insert into table1 select 6, 13, 11")
      sql("insert into table1 select 6, 9, 10")
      sql("insert into table1 select 6, 15, 8")
      sql("insert into table1 select 6, 10, 1")
      sql("insert into table1 select 6, 15, 8")
      sql("insert into table1 select 6, 7, 4")
      sql("insert into table1 select 6, 7, 8")

      //sliding frame with exclude current row
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between 2 preceding and 2 following exclude current row)" +
        " from table1 where col1 = 6"),
        Seq(
          Row(6, 10, 1, 18),
          Row(6, 11, 4, 32),
          Row(6, 7, 4, 51),
          Row(6, 15, 8, 40),
          Row(6,  15,   8, 41),
          Row(6,   7,   8, 51),
          Row(6,  12,  10, 44),
          Row(6,   9,  10, 32),
          Row(6,  13,  11, 21)
        )
      )

      //sliding frame with exclude group
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between 2 preceding and " +
        " 2 following exclude group) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1, 18),
          Row(6,  11,   4, 25),
          Row(6,   7,   4, 40),
          Row(6,  15,   8, 18),
          Row(6,  15,   8, 19),
          Row(6,   7,   8, 21),
          Row(6,  12,  10, 35),
          Row(6,   9,  10, 20),
          Row(6,  13,  11, 21)
        )
      )

      //sliding frame with exclude ties
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between 2 preceding and " +
        " 2 following exclude ties) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1, 28),
          Row(6,  11,   4, 36),
          Row(6,   7,   4, 47),
          Row(6,  15,   8, 33),
          Row(6,  15,   8, 34),
          Row(6,   7,   8, 28),
          Row(6,  12,  10, 47),
          Row(6,   9,  10, 29),
          Row(6,  13,  11, 34)       
        )
      )

      //sliding frame with exclude no others
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between 2 preceding and " +
        " 2 following) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1, 28),
          Row(6,  11,   4, 43),
          Row(6,   7,   4, 58),
          Row(6,  15,   8, 55),
          Row(6,  15,   8, 56),
          Row(6,   7,   8, 58),
          Row(6,  12,  10, 56),
          Row(6,   9,  10, 41),
          Row(6,  13,  11, 34)
        )
      )


      //expanding frame with exclude current row
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between unbounded preceding and " +
        " current row exclude current row) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1,  null),
          Row(6,  11,   4,    10),
          Row(6,   7,   4,    21),
          Row(6,  15,   8,    28),
          Row(6,  15,   8,    43),
          Row(6,   7,   8,    58),
          Row(6,  12,  10,    65),
          Row(6,   9,  10,    77),
          Row(6,  13,  11,    86)
        )
      )

      //expanding frame with exclude group
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between unbounded preceding and " +
        " current row exclude group) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1,  null),
          Row(6,  11,   4,    10),
          Row(6,   7,   4,    10),
          Row(6,  15,   8,    28),
          Row(6,  15,   8,    28),
          Row(6,   7,   8,    28),
          Row(6,  12,  10,    65),
          Row(6,   9,  10,    65),
          Row(6,  13,  11,    86)
        )
      )

      //expanding frame with exclude ties
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between unbounded preceding and " +
        " current row exclude ties) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1,    10),
          Row(6,  11,   4,    21),
          Row(6,   7,   4,    17),
          Row(6,  15,   8,    43),
          Row(6,  15,   8,    43),
          Row(6,   7,   8,    35),
          Row(6,  12,  10,    77),
          Row(6,   9,  10,    74),
          Row(6,  13,  11,    99)
        )
      )
      
      //expanding frame with exclude no others
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between unbounded preceding and " +
        " current row) from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1,    10),
          Row(6,  11,   4,    21),
          Row(6,   7,   4,    28),
          Row(6,  15,   8,    43), 
          Row(6,  15,   8,    58),
          Row(6,   7,   8,    65),
          Row(6,  12,  10,    77),
          Row(6,   9,  10,    86),
          Row(6,  13,  11,    99)
        )
      )

      //shrinking frame with exclude current row
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between current row and " +
        " unbounded following exclude current row) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1,    89),
          Row(6,  11,   4,    78),
          Row(6,   7,   4,    71),
          Row(6,  15,   8,    56),
          Row(6,  15,   8,    41),
          Row(6,   7,   8,    34),
          Row(6,  12,  10,    22),
          Row(6,   9,  10,    13),
          Row(6,  13,  11,  null)
        )
      )

      //shrinking frame with exclude current group
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between current row and " +
        " unbounded following exclude group) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1,    89),
          Row(6,  11,   4,    71),
          Row(6,   7,   4,    71),
          Row(6,  15,   8,    34),
          Row(6,  15,   8,    34),
          Row(6,   7,   8,    34),
          Row(6,  12,  10,    13),
          Row(6,   9,  10,    13),
          Row(6,  13,  11,  null)
        )
      )
      //shrinking frame with exclude current ties
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between current row and " +
        " unbounded following exclude ties) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1,    99),
          Row(6,  11,   4,    82),
          Row(6,   7,   4,    78),
          Row(6,  15,   8,    49),
          Row(6,  15,   8,    49),
          Row(6,   7,   8,    41),
          Row(6,  12,  10,    25),
          Row(6,   9,  10,    22),
          Row(6,  13,  11,    13)
        )
      )

      //shrinking frame with exclude current no others
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between current row and " +
        " unbounded following) from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1,    99),
          Row(6,  11,   4,    89),
          Row(6,   7,   4,    78),
          Row(6,  15,   8,    71),
          Row(6,  15,   8,    56),
          Row(6,   7,   8,    41),
          Row(6,  12,  10,    34),
          Row(6,   9,  10,    22),
          Row(6,  13,  11,    13)
        )
      )


      //whole partition frame with exclude current row
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between unbounded preceding and " +
        " unbounded following exclude current row) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1,    89),
          Row(6,  11,   4,    88),
          Row(6,   7,   4,    92), 
          Row(6,  15,   8,    84),
          Row(6,  15,   8,    84),
          Row(6,   7,   8,    92),
          Row(6,  12,  10,    87),
          Row(6,   9,  10,    90),
          Row(6,  13,  11,    86)
        )
      )

      //whole partition frame with exclude group
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between unbounded preceding and " +
        " unbounded following exclude group) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1,    89),
          Row(6,  11,   4,    81),
          Row(6,   7,   4,    81),
          Row(6,  15,   8,    62),
          Row(6,  15,   8,    62),
          Row(6,   7,   8,    62),
          Row(6,  12,  10,    78),
          Row(6,   9,  10,    78),
          Row(6,  13,  11,    86)
        )
      )

      //whole partition frame with exclude ties
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between unbounded preceding and " +
        " unbounded following exclude ties) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1,    99),
          Row(6,  11,   4,    92),
          Row(6,   7,   4,    88),
          Row(6,  15,   8,    77),
          Row(6,  15,   8,    77),
          Row(6,   7,   8,    69), 
          Row(6,  12,  10,    90),
          Row(6,   9,  10,    87),
          Row(6,  13,  11,    99)
        )
      )

      //whole partition frame with exclude no others
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 rows between unbounded preceding and " +
        " unbounded following) from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1,    99),
          Row(6,  11,   4,    99),
          Row(6,   7,   4,    99),
          Row(6,  15,   8,    99),
          Row(6,  15,   8,    99),
          Row(6,   7,   8,    99),
          Row(6,  12,  10,    99),
          Row(6,   9,  10,    99),
          Row(6,  13,  11,    99)
        )
      )

      // sliding range framing
      checkAnswer(sql("select col1, col2, col3, sum(col2) over" +
        " (partition by col1 order by col3 range between 3 preceding and 3 following) " +
        " from table1 where col1 = 6"), 
        Seq(
          Row(6,  10,   1, 28),
          Row(6,  11,   4, 28),
          Row(6,   7,   4, 28),
          Row(6,  15,   8, 71),
          Row(6,  15,   8, 71),
          Row(6,   7,   8, 71),
          Row(6,  12,  10, 71),
          Row(6,   9,  10, 71),
          Row(6,  13,  11, 71)
        )
      )

      // sliding range framing with exclude current row
      checkAnswer(sql("select col1, col2, col3, sum(col2) over" +
        " (partition by col1 order by col3 range between 3 preceding and 3 following " +
        " exclude current row ) " +
        " from table1 where col1 = 6"),
        Seq(
          Row(6,  10,   1, 18),
          Row(6,  11,   4, 17),
          Row(6,   7,   4, 21),
          Row(6,  15,   8, 56),
          Row(6,  15,   8, 56),
          Row(6,   7,   8, 64),
          Row(6,  12,  10, 59),
          Row(6,   9,  10, 62),
          Row(6,  13,  11, 58)
        )
      )

      // sliding range framing with exclude group
      checkAnswer(sql("select col1, col2, col3, sum(col2) over" +
        " (partition by col1 order by col3 range between 3 preceding and 3 following " +
        " exclude group ) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1, 18),
           Row(6,  11,   4, 10),
           Row(6,   7,   4, 10),
           Row(6,  15,   8, 34),
           Row(6,  15,   8, 34),
           Row(6,   7,   8, 34),
           Row(6,  12,  10, 50),
           Row(6,   9,  10, 50),
           Row(6,  13,  11, 58)
        )
      )

      // sliding range framing with exclude ties
      checkAnswer(sql("select col1, col2, col3, sum(col2) over" +
        " (partition by col1 order by col3 range between 3 preceding and 3 following " +
        " exclude ties) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,     28),
           Row(6,  11,   4,     21),
           Row(6,   7,   4,     17),
           Row(6,  15,   8,     49),
           Row(6,  15,   8,     49),
           Row(6,   7,   8,     41),
           Row(6,  12,  10,     62),
           Row(6,   9,  10,     59),
           Row(6,  13,  11,     71)
        )
      )

      // expanding range framing
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 range between unbounded preceding and " +
        " current row) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,     10),
           Row(6,  11,   4,     28),
           Row(6,   7,   4,     28),
           Row(6,  15,   8,     65),
           Row(6,  15,   8,     65),
           Row(6,   7,   8,     65),
           Row(6,  12,  10,     86),
           Row(6,   9,  10,     86),
           Row(6,  13,  11,     99)
        )
      )

      // expanding range framing with exclude current row 
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 range between unbounded preceding and " +
        " current row exclude current row) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,     null),
           Row(6,  11,   4,     17),
           Row(6,   7,   4,     21),
           Row(6,  15,   8,     50),
           Row(6,  15,   8,     50),
           Row(6,   7,   8,     58),
           Row(6,  12,  10,     74),
           Row(6,   9,  10,     77),
           Row(6,  13,  11,     86)
        )
      )

      // expanding range framing with exclude group
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 range between unbounded preceding and " +
        " current row exclude group) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,   null),
           Row(6,  11,   4,     10),
           Row(6,   7,   4,     10),
           Row(6,  15,   8,     28),
           Row(6,  15,   8,     28),
           Row(6,   7,   8,     28),
           Row(6,  12,  10,     65),
           Row(6,   9,  10,     65),
           Row(6,  13,  11,     86)
        )
      )

      // expanding range framing with exclude ties
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 range between unbounded preceding and " +
        " current row exclude ties) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,     10),
           Row(6,  11,   4,     21),
           Row(6,   7,   4,     17),
           Row(6,  15,   8,     43),
           Row(6,  15,   8,     43),
           Row(6,   7,   8,     35),
           Row(6,  12,  10,     77),
           Row(6,   9,  10,     74),
           Row(6,  13,  11,     99)
        )
      )

      // shrinking range framing
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 range between current row and " +
        " unbounded following ) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,     99),
           Row(6,  11,   4,     89),
           Row(6,   7,   4,     89),
           Row(6,  15,   8,     71),
           Row(6,  15,   8,     71),
           Row(6,   7,   8,     71),
           Row(6,  12,  10,     34),
           Row(6,   9,  10,     34),
           Row(6,  13,  11,     13)
        )
      )
        
      // shrinking range framing with exclude current row
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 range between current row and " +
        " unbounded following exclude current row) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,     89),
           Row(6,  11,   4,     78),
           Row(6,   7,   4,     82),
           Row(6,  15,   8,     56),
           Row(6,  15,   8,     56),
           Row(6,   7,   8,     64),
           Row(6,  12,  10,     22),
           Row(6,   9,  10,     25),
           Row(6,  13,  11,   null)
        )
      )
      
      // shrinking range framing with exclude group
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 range between current row and " +
        " unbounded following exclude group) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,     89),
           Row(6,  11,   4,     71),
           Row(6,   7,   4,     71),
           Row(6,  15,   8,     34),
           Row(6,  15,   8,     34),
           Row(6,   7,   8,     34),
           Row(6,  12,  10,     13),
           Row(6,   9,  10,     13),
           Row(6,  13,  11,   null)
        )
      )
      
      // shrinking range framing with exclude ties
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 range between current row and " +
        " unbounded following exclude ties) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,     99),
           Row(6,  11,   4,     82),
           Row(6,   7,   4,     78),
           Row(6,  15,   8,     49),
           Row(6,  15,   8,     49),
           Row(6,   7,   8,     41),
           Row(6,  12,  10,     25),
           Row(6,   9,  10,     22),
           Row(6,  13,  11,     13)
        )
      )

      // whole partition range framing 
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 range between unbounded preceding and " +
        " unbounded following ) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,     99),
           Row(6,  11,   4,     99),
           Row(6,   7,   4,     99),
           Row(6,  15,   8,     99),
           Row(6,  15,   8,     99),
           Row(6,   7,   8,     99),
           Row(6,  12,  10,     99),
           Row(6,   9,  10,     99),
           Row(6,  13,  11,     99)
        )
      )
      
      // whole partition range framing with exclude current row
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 range between unbounded preceding and " +
        " unbounded following exclude current row) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,     89),
           Row(6,  11,   4,     88),
           Row(6,   7,   4,     92),
           Row(6,  15,   8,     84),
           Row(6,  15,   8,     84),
           Row(6,   7,   8,     92),
           Row(6,  12,  10,     87),
           Row(6,   9,  10,     90),
           Row(6,  13,  11,     86)
        )
      )
      
      // whole partition range framing with exclude group
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 range between unbounded preceding and " +
        " unbounded following exclude group) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,     89),
           Row(6,  11,   4,     81),
           Row(6,   7,   4,     81),
           Row(6,  15,   8,     62),
           Row(6,  15,   8,     62),
           Row(6,   7,   8,     62),
           Row(6,  12,  10,     78),
           Row(6,   9,  10,     78),
           Row(6,  13,  11,     86)
        )
      )
        
      // whole partition range framing with exclude ties
      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 range between unbounded preceding and " +
        " unbounded following exclude ties) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,     99),
           Row(6,  11,   4,     92),
           Row(6,   7,   4,     88),
           Row(6,  15,   8,     77),
           Row(6,  15,   8,     77),
           Row(6,   7,   8,     69),
           Row(6,  12,  10,     90),
           Row(6,   9,  10,     87),
           Row(6,  13,  11,     99)
        )
      )

      // trying on other aggregation functions, like AVG, MIN, MAX
      checkAnswer(sql("select col1, col2, col3, AVG(col2) over " +
        " (partition by col1 order by col3 exclude current row) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1, null),
           Row(6,  11,   4,  8.5),
           Row(6,   7,   4, 10.5),
           Row(6,  15,   8, 10.0),
           Row(6,  15,   8, 10.0),
           Row(6,   7,   8, 11.6),
           Row(6,  12,  10,     10.571428571428571),
           Row(6,   9,  10, 11.0),
           Row(6,  13,  11,10.75)
        )
      )

      checkAnswer(sql("select col1, col2, col3, MIN(col2) over " +
        " (partition by col1 order by col3 exclude group) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,   null),
           Row(6,  11,   4,     10),
           Row(6,   7,   4,     10),
           Row(6,  15,   8,      7),
           Row(6,  15,   8,      7),
           Row(6,   7,   8,      7),
           Row(6,  12,  10,      7),
           Row(6,   9,  10,      7),
           Row(6,  13,  11,      7)
        )
      )

      checkAnswer(sql("select col1, col2, col3, MAX(col2) over " +
        " (partition by col1 order by col3 exclude current row) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,   null),
           Row(6,  11,   4,     10),
           Row(6,   7,   4,     11),
           Row(6,  15,   8,     15),
           Row(6,  15,   8,     15),
           Row(6,   7,   8,     15),
           Row(6,  12,  10,     15),
           Row(6,   9,  10,     15),
           Row(6,  13,  11,     15)
        )
      )

      checkAnswer(sql("select col1, col2, col3, MAX(col2) over " +
        " (partition by col1 order by col3 exclude group) " +
        " from table1 where col1 = 6"),
        Seq(
           Row(6,  10,   1,   null),
           Row(6,  11,   4,     10),
           Row(6,   7,   4,     10),
           Row(6,  15,   8,     11),
           Row(6,  15,   8,     11),
           Row(6,   7,   8,     11),
           Row(6,  12,  10,     15),
           Row(6,   9,  10,     15),
           Row(6,  13,  11,     15)
        )
      )

      // more than one partition
      sql("create table table2 (col1 int, col2 int, col3 int)")
      sql("insert into table2 select 6, 12, 10")
      sql("insert into table2 select 6, 11, 4")
      sql("insert into table2 select 6, 13, 11")
      sql("insert into table2 select 6, 9, 10")
      sql("insert into table2 select 6, 15, 8")
      sql("insert into table2 select 6, 10, 1")
      sql("insert into table2 select 6, 15, 8")
      sql("insert into table2 select 6, 7, 4")
      sql("insert into table2 select 6, 7, 8")
      sql("insert into table2 select 7, 12, 10")
      sql("insert into table2 select 7, 11, 4")
      sql("insert into table2 select 7, 13, 11")
      sql("insert into table2 select 7, 9, 10")
      sql("insert into table2 select 7, 15, 8")
      sql("insert into table2 select 7, 10, 1")
      sql("insert into table2 select 7, 15, 8")
      sql("insert into table2 select 7, 7, 4")
      sql("insert into table2 select 7, 7, 8")

      checkAnswer(sql("select col1, col2, col3, sum(col2) over " +
        " (partition by col1 order by col3 exclude current row )" +
        " from table2 where col1 < 20 order by col1"),
        Seq(
           Row(6,  10,   1, null),
           Row(6,  11,   4,   17),
           Row(6,   7,   4,   21),
           Row(6,  15,   8,   50),
           Row(6,  15,   8,   50),
           Row(6,   7,   8,   58),
           Row(6,  12,  10,   74),
           Row(6,   9,  10,   77),
           Row(6,  13,  11,   86),
           Row(7,  10,   1, null),
           Row(7,  11,   4,   17),
           Row(7,   7,   4,   21),
           Row(7,   7,   8,   58),
           Row(7,  15,   8,   50),
           Row(7,  15,   8,   50),
           Row(7,   9,  10,   77),
           Row(7,  12,  10,   74),
           Row(7,  13,  11,   86)
        )
      )
    }
  }
  test("Non windowAggregation with exclude clause") {
    withTable("table1", "table2") {
      sql("create table table1 (col1 int, col2 int, col3 int)")
      sql("insert into table1 select 6, 12, 10")
      sql("insert into table1 select 6, 11, 4")
      sql("insert into table1 select 6, 13, 11")
      sql("insert into table1 select 6, 9, 10")
      sql("insert into table1 select 6, 15, 8")
      sql("insert into table1 select 6, 10, 1")
      sql("insert into table1 select 6, 15, 8")
      sql("insert into table1 select 6, 7, 4")
      sql("insert into table1 select 6, 7, 8")

      try {
        sql("select col1, cume_dist() over " +
          "(partition by col1 order by col3 exclude current row) " +
          "from table1 ")
      } catch {
        case ae: AnalysisException =>
          assert(ae.getMessage.contains("does not support exclude clause"))
      }

      try {
        sql("select col1, rank() over " +
          "(partition by col1 order by col3 exclude current row) " +
          "from table1 ")
      } catch {
        case ae: AnalysisException =>
          assert(ae.getMessage.contains("does not support exclude clause"))
      }

      try {
        sql("select col1, lead(2) over " +
          "(partition by col1 order by col3 exclude current row) " +
          "from table1 ")
      } catch {
        case ae: AnalysisException =>
          assert(ae.getMessage.contains("does not support exclude clause"))
      }
    }
  }

  test("SPARK-16646: LAST_VALUE(FALSE) OVER ()") {
    checkAnswer(sql("SELECT LAST_VALUE(FALSE) OVER ()"), Row(false))
    checkAnswer(sql("SELECT LAST_VALUE(FALSE, FALSE) OVER ()"), Row(false))
    checkAnswer(sql("SELECT LAST_VALUE(TRUE, TRUE) OVER ()"), Row(true))
  }

  test("SPARK-16646: FIRST_VALUE(FALSE) OVER ()") {
    checkAnswer(sql("SELECT FIRST_VALUE(FALSE) OVER ()"), Row(false))
    checkAnswer(sql("SELECT FIRST_VALUE(FALSE, FALSE) OVER ()"), Row(false))
    checkAnswer(sql("SELECT FIRST_VALUE(TRUE, TRUE) OVER ()"), Row(true))
  }
}
