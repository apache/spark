package catalyst

import java.io.File
import shark.SharkEnv

object TestUtils {
  def getTempFilePath(prefix: String, suffix: String = ""): File = {
    val tempFile = File.createTempFile(prefix, suffix)
    tempFile.delete()
    tempFile
  }

  def getTestContext = {
    val WAREHOUSE_PATH = TestUtils.getTempFilePath("sharkWarehouse")
    val METASTORE_PATH = TestUtils.getTempFilePath("sharkMetastore")
    val MASTER = "local"

    val sc = SharkEnv.initWithSharkContext("shark-sql-suite-testing", MASTER)

    sc.runSql("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" + METASTORE_PATH + ";create=true")
    sc.runSql("set hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)

    // test
    //sc.runSql("DROP TABLE IF EXISTS test")
    sc.runSql("CREATE TABLE test (key INT, val STRING)")
    sc.runSql("""LOAD DATA LOCAL INPATH '/Users/marmbrus/workspace/hive/data/files/kv1.txt' INTO TABLE test""")

    sc
  }
}