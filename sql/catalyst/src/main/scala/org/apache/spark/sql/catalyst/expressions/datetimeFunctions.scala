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

import java.text.SimpleDateFormat
import java.util.{TimeZone, SimpleTimeZone}

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds), using the
 * default timezone and the default locale, return 0 if fail:
 * unix_timestamp('2009-03-20 11:30:01') = 1237573801
 */
case class UnixTimestamp(date: Expression, format: Expression)
  extends Expression with ExpectsInputTypes {
  override def children: Seq[Expression] = date :: format :: Nil

  override def foldable: Boolean = date.foldable && format.foldable
  override def nullable: Boolean = format.nullable || format.nullable

  override def expectedChildTypes: Seq[DataType] = StringType :: StringType :: Nil

  override def dataType: DataType = LongType

  override def eval(input: InternalRow): Any = {
    val dateVal = date.eval(input)
    val formatVal = format.eval(input)
    if (dateVal == null || formatVal == null) {
      null
    } else {
      def sdf = new SimpleDateFormat(formatVal.asInstanceOf[UTF8String].toString)
      sdf.parse(dateVal.asInstanceOf[UTF8String].toString).getTime / 1000L
    }
  }

}

/**
 * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
 * representing the timestamp of that moment in the current system time zone in the format
 * of "1970-01-01 00:00:00".
 */
case class FromUnixTimestamp(unixTime: Expression, format: Expression)
  extends Expression with ExpectsInputTypes {
  override def children: Seq[Expression] = unixTime :: format :: Nil

  override def foldable: Boolean = unixTime.foldable && format.foldable
  override def nullable: Boolean = unixTime.nullable || format.nullable

  override def expectedChildTypes: Seq[DataType] = LongType :: StringType :: Nil

  override def dataType: DataType = StringType

  override def eval(input: InternalRow): Any = {
    val dateVal = unixTime.eval(input)
    val formatVal = format.eval(input)
    if (dateVal == null || formatVal == null) {
      null
    } else {
      def sdf = new SimpleDateFormat(formatVal.asInstanceOf[UTF8String].toString)
      UTF8String.fromString(sdf.format(new java.util.Date(unixTime.asInstanceOf[Long] * 1000L)))
    }
  }

}

case class FromUTCTimestamp(time: Expression, timezone: Expression)
  extends Expression with ExpectsInputTypes {
  override def children: Seq[Expression] = time :: timezone :: Nil

  override def foldable: Boolean = time.foldable && timezone.foldable
  override def nullable: Boolean = timezone.nullable || timezone.nullable

  override def expectedChildTypes: Seq[DataType] = StringType :: StringType :: Nil

  override def dataType: DataType = StringType

  override def eval(input: InternalRow): Any = {
    val timeVal = time.eval(input)
    val tzVal = timezone.eval(input)
    if (timeVal == null || tzVal == null) {
      null
    } else {
      def sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
      val unixtime = sdf.parse(timeVal.asInstanceOf[UTF8String].toString).getTime
      sdf.setTimeZone(TimeZone.getTimeZone(timezone.asInstanceOf[UTF8String].toString))
      UTF8String.fromString(sdf.format(new java.util.Date(unixtime)))
    }
  }

}

case class ToUTCTimestamp(time: Expression, timezone: Expression)
  extends Expression with ExpectsInputTypes {
  override def children: Seq[Expression] = time :: timezone :: Nil

  override def foldable: Boolean = time.foldable && timezone.foldable
  override def nullable: Boolean = timezone.nullable || timezone.nullable

  override def expectedChildTypes: Seq[DataType] = StringType :: StringType :: Nil

  override def dataType: DataType = StringType

  override def eval(input: InternalRow): Any = {
    val timeVal = time.eval(input)
    val tzVal = timezone.eval(input)
    if (timeVal == null || tzVal == null) {
      null
    } else {
      def sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      sdf.setTimeZone(TimeZone.getTimeZone(timezone.asInstanceOf[UTF8String].toString))
      val unixtime = sdf.parse(timeVal.asInstanceOf[UTF8String].toString).getTime
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
      UTF8String.fromString(sdf.format(new java.util.Date(unixtime)))
    }
  }

}