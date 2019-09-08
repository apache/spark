/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli

import org.apache.spark.service.cli.thrift.TFetchOrientation

trait FetchOrientation {
  def toTFetchOrientation: TFetchOrientation
}

object FetchOrientation {

  case object FETCH_NEXT extends FetchOrientation {
    override val toTFetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_NEXT
  }

  case object FETCH_PRIOR extends FetchOrientation {
    override val toTFetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_PRIOR
  }

  case object FETCH_RELATIVE extends FetchOrientation {
    override val toTFetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_RELATIVE
  }

  case object FETCH_ABSOLUTE extends FetchOrientation {
    override val toTFetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_ABSOLUTE
  }

  case object FETCH_FIRST extends FetchOrientation {
    override val toTFetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_FIRST
  }

  case object FETCH_LAST extends FetchOrientation {
    override val toTFetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_LAST
  }

  def getFetchOrientation(tFetchOrientation: TFetchOrientation): FetchOrientation =
    tFetchOrientation match {
      case FETCH_FIRST.toTFetchOrientation => FETCH_FIRST
      case _ => FETCH_NEXT
    }
}
