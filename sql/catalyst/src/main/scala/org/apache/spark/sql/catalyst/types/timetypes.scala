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

package org.apache.spark.sql.catalyst.expressions

import java.sql.{Date, Timestamp}
import scala.language.implicitConversions

/* *
 * Subclass of java.sql.Date which provides the usual comparison
 * operators (as required for catalyst expressions) and which can
 * be constructed from a string.
 *
 * scala> val d1 = Date("2014-02-01")
 * d1: Date = 2014-02-01
 *
 * scala> val d2 = Date("2014-02-02")
 * d2: Date = 2014-02-02
 *
 * scala> d1 < d2
 * res1: Boolean = true
 */

class RichDate(milliseconds: Long) extends Date(milliseconds) {
  def < (that: Date): Boolean = this.before(that)
  def > (that: Date): Boolean  = this.after(that)
  def <= (that: Date): Boolean = (this.before(that) || this.equals(that))
  def >= (that: Date): Boolean = (this.after(that) || this.equals(that))
  def === (that: Date): Boolean = this.equals(that)
}

object RichDate {
  def apply(init: String) = new RichDate(Date.valueOf(init).getTime)

  def unapply(richdate: RichDate): Option[Date] = Some(new Date(richdate.getTime)) 
}

/* *
 * Analogous subclass of java.sql.Timestamp.
 *
 * scala> val ts1 = Timestamp("2014-03-04 12:34:56.12")
 * ts1: Timestamp = 2014-03-04 12:34:56.12
 *
 * scala> val ts2 = Timestamp("2014-03-04 12:34:56.13")
 * ts2: Timestamp = 2014-03-04 12:34:56.13
 *
 * scala> ts1 < ts2
 * res13: Boolean = true
 */

class RichTimestamp(milliseconds: Long) extends Timestamp(milliseconds) {
  def < (that: Timestamp): Boolean = this.before(that)
  def > (that: Timestamp): Boolean  = this.after(that)
  def <= (that: Timestamp): Boolean = (this.before(that) || this.equals(that))
  def >= (that: Timestamp): Boolean = (this.after(that) || this.equals(that))
  def === (that: Timestamp): Boolean = this.equals(that)
  // Follow Hive conventions when converting to a String.
  // Copied from the Cast class.
  override def toString(): String = { 
    val jts = new Timestamp(this.getTime)
    val timestampString = jts.toString
    val formatted = Cast.threadLocalTimestampFormat.get.format(jts)
    
    if (timestampString.length > 19 && timestampString.substring(19) != ".0") {
      formatted + timestampString.substring(19)
    } else {
      formatted
    }
  }
}

object RichTimestamp {
  def apply(init: String) = new RichTimestamp(Timestamp.valueOf(init).getTime)

  def unapply(richtimestamp: RichTimestamp): Option[Timestamp] = 
    Some(new Timestamp(richtimestamp.getTime)) 
}

/* *
 * Implicit conversions.
 */

object TimeConversions {

  implicit def javaDateToRichDate(jdate: Date): RichDate = {
    new RichDate(jdate.getTime)
  }

  implicit def javaTimestampToRichTimestamp(jtimestamp: Timestamp): RichTimestamp = {
    new RichTimestamp(jtimestamp.getTime)
  }

  implicit def richDateToJavaDate(date: RichDate): Date = {
    new Date(date.getTime)
  }

  implicit def richTimestampToJavaTimestamp(timestamp: RichTimestamp): Timestamp = {
    new Timestamp(timestamp.getTime)
  }

}
