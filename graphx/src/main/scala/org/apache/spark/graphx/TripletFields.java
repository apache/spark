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

package org.apache.spark.graphx;

import java.io.Serializable;

/**
 * Represents a subset of the fields of an [[EdgeTriplet]] or [[EdgeContext]]. This allows the
 * system to populate only those fields for efficiency.
 */
public class TripletFields implements Serializable {
  public final boolean useSrc;
  public final boolean useDst;
  public final boolean useEdge;

  public TripletFields() {
    this(true, true, true);
  }

  public TripletFields(boolean useSrc, boolean useDst, boolean useEdge) {
    this.useSrc = useSrc;
    this.useDst = useDst;
    this.useEdge = useEdge;
  }

  public static final TripletFields None = new TripletFields(false, false, false);
  public static final TripletFields EdgeOnly = new TripletFields(false, false, true);
  public static final TripletFields SrcOnly = new TripletFields(true, false, false);
  public static final TripletFields DstOnly = new TripletFields(false, true, false);
  public static final TripletFields SrcDstOnly = new TripletFields(true, true, false);
  public static final TripletFields SrcAndEdge = new TripletFields(true, false, true);
  public static final TripletFields Src = SrcAndEdge;
  public static final TripletFields DstAndEdge = new TripletFields(false, true, true);
  public static final TripletFields Dst = DstAndEdge;
  public static final TripletFields All = new TripletFields(true, true, true);
}
