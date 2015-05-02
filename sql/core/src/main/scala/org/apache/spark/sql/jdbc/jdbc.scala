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

package org.apache.spark.sql

import java.sql.{Connection, Driver, DriverManager, DriverPropertyInfo, PreparedStatement, SQLFeatureNotSupportedException}
import java.util.Properties

import scala.collection.mutable

import org.apache.spark.Logging
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

package object jdbc {
  private[sql] object JDBCWriteDetails extends Logging {
    /**
     * Returns a PreparedStatement that inserts a row into table via conn.
     */
    def insertStatement(conn: Connection, table: String, rddSchema: StructType):
        PreparedStatement = {
      val sql = new StringBuilder(s"INSERT INTO $table VALUES (")
      var fieldsLeft = rddSchema.fields.length
      while (fieldsLeft > 0) {
        sql.append("?")
        if (fieldsLeft > 1) sql.append(", ") else sql.append(")")
        fieldsLeft = fieldsLeft - 1
      }
      conn.prepareStatement(sql.toString)
    }

    /**
     * Saves a partition of a DataFrame to the JDBC database.  This is done in
     * a single database transaction in order to avoid repeatedly inserting
     * data as much as possible.
     *
     * It is still theoretically possible for rows in a DataFrame to be
     * inserted into the database more than once if a stage somehow fails after
     * the commit occurs but before the stage can return successfully.
     *
     * This is not a closure inside saveTable() because apparently cosmetic
     * implementation changes elsewhere might easily render such a closure
     * non-Serializable.  Instead, we explicitly close over all variables that
     * are used.
     */
    def savePartition(url: String, table: String, iterator: Iterator[Row],
        rddSchema: StructType, nullTypes: Array[Int]): Iterator[Byte] = {
      val conn = DriverManager.getConnection(url)
      var committed = false
      try {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        val stmt = insertStatement(conn, table, rddSchema)
        try {
          while (iterator.hasNext) {
            val row = iterator.next()
            val numFields = rddSchema.fields.length
            var i = 0
            while (i < numFields) {
              if (row.isNullAt(i)) {
                stmt.setNull(i + 1, nullTypes(i))
              } else {
                rddSchema.fields(i).dataType match {
                  case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                  case LongType => stmt.setLong(i + 1, row.getLong(i))
                  case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                  case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                  case ShortType => stmt.setInt(i + 1, row.getShort(i))
                  case ByteType => stmt.setInt(i + 1, row.getByte(i))
                  case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                  case StringType => stmt.setString(i + 1, row.getString(i))
                  case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                  case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                  case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                  case DecimalType.Unlimited => stmt.setBigDecimal(i + 1,
                      row.getAs[java.math.BigDecimal](i))
                  case _ => throw new IllegalArgumentException(
                      s"Can't translate non-null value for field $i")
                }
              }
              i = i + 1
            }
            stmt.executeUpdate()
          }
        } finally {
          stmt.close()
        }
        conn.commit()
        committed = true
      } finally {
        if (!committed) {
          // The stage must fail.  We got here through an exception path, so
          // let the exception through unless rollback() or close() want to
          // tell the user about another problem.
          conn.rollback()
          conn.close()
        } else {
          // The stage must succeed.  We cannot propagate any exception close() might throw.
          try {
            conn.close()
          } catch {
            case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
          }
        }
      }
      Array[Byte]().iterator
    }

    /**
     * Compute the schema string for this RDD.
     */
    def schemaString(df: DataFrame, url: String): String = {
      val sb = new StringBuilder()
      val quirks = DriverQuirks.get(url)
      df.schema.fields foreach { field => {
        val name = field.name
        var typ: String = quirks.getJDBCType(field.dataType)._1
        if (typ == null) typ = field.dataType match {
          case IntegerType => "INTEGER"
          case LongType => "BIGINT"
          case DoubleType => "DOUBLE PRECISION"
          case FloatType => "REAL"
          case ShortType => "INTEGER"
          case ByteType => "BYTE"
          case BooleanType => "BIT(1)"
          case StringType => "TEXT"
          case BinaryType => "BLOB"
          case TimestampType => "TIMESTAMP"
          case DateType => "DATE"
          case DecimalType.Unlimited => "DECIMAL(40,20)"
          case _ => throw new IllegalArgumentException(s"Don't know how to save $field to JDBC")
        }
        val nullable = if (field.nullable) "" else "NOT NULL"
        sb.append(s", $name $typ $nullable")
      }}
      if (sb.length < 2) "" else sb.substring(2)
    }

    /**
     * Saves the RDD to the database in a single transaction.
     */
    def saveTable(df: DataFrame, url: String, table: String) {
      val quirks = DriverQuirks.get(url)
      var nullTypes: Array[Int] = df.schema.fields.map(field => {
        var nullType: Option[Int] = quirks.getJDBCType(field.dataType)._2
        if (nullType.isEmpty) {
          field.dataType match {
            case IntegerType => java.sql.Types.INTEGER
            case LongType => java.sql.Types.BIGINT
            case DoubleType => java.sql.Types.DOUBLE
            case FloatType => java.sql.Types.REAL
            case ShortType => java.sql.Types.INTEGER
            case ByteType => java.sql.Types.INTEGER
            case BooleanType => java.sql.Types.BIT
            case StringType => java.sql.Types.CLOB
            case BinaryType => java.sql.Types.BLOB
            case TimestampType => java.sql.Types.TIMESTAMP
            case DateType => java.sql.Types.DATE
            case DecimalType.Unlimited => java.sql.Types.DECIMAL
            case _ => throw new IllegalArgumentException(
              s"Can't translate null value for field $field")
          }
        } else nullType.get
      }).toArray

      val rddSchema = df.schema
      df.foreachPartition { iterator =>
        JDBCWriteDetails.savePartition(url, table, iterator, rddSchema, nullTypes)
      }
    }

  }

  private [sql] class DriverWrapper(val wrapped: Driver) extends Driver {
    override def acceptsURL(url: String): Boolean = wrapped.acceptsURL(url)

    override def jdbcCompliant(): Boolean = wrapped.jdbcCompliant()

    override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = {
      wrapped.getPropertyInfo(url, info)
    }

    override def getMinorVersion: Int = wrapped.getMinorVersion

    def getParentLogger: java.util.logging.Logger =
      throw new SQLFeatureNotSupportedException(
        s"${this.getClass().getName}.getParentLogger is not yet implemented.")

    override def connect(url: String, info: Properties): Connection = wrapped.connect(url, info)

    override def getMajorVersion: Int = wrapped.getMajorVersion
  }

  /**
   * java.sql.DriverManager is always loaded by bootstrap classloader,
   * so it can't load JDBC drivers accessible by Spark ClassLoader.
   *
   * To solve the problem, drivers from user-supplied jars are wrapped
   * into thin wrapper.
   */
  private [sql] object DriverRegistry extends Logging {

    private val wrapperMap: mutable.Map[String, DriverWrapper] = mutable.Map.empty

    def register(className: String): Unit = {
      val cls = Utils.getContextOrSparkClassLoader.loadClass(className)
      if (cls.getClassLoader == null) {
        logTrace(s"$className has been loaded with bootstrap ClassLoader, wrapper is not required")
      } else if (wrapperMap.get(className).isDefined) {
        logTrace(s"Wrapper for $className already exists")
      } else {
        synchronized {
          if (wrapperMap.get(className).isEmpty) {
            val wrapper = new DriverWrapper(cls.newInstance().asInstanceOf[Driver])
            DriverManager.registerDriver(wrapper)
            wrapperMap(className) = wrapper
            logTrace(s"Wrapper for $className registered")
          }
        }
      }
    }
    
    def getDriverClassName(url: String): String = DriverManager.getDriver(url) match {
      case wrapper: DriverWrapper => wrapper.wrapped.getClass.getCanonicalName
      case driver => driver.getClass.getCanonicalName  
    }
  }

} // package object jdbc
