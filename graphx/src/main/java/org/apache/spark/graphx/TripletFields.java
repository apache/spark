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

  /** Indicates whether the source vertex attribute is included. */
  public final boolean useSrc;

  /** Indicates whether the destination vertex attribute is included. */
  public final boolean useDst;

  /** Indicates whether the edge attribute is included. */
  public final boolean useEdge;

  /** Constructs a default TripletFields in which all fields are included. */
  public TripletFields() {
    this(true, true, true);
  }

  public TripletFields(boolean useSrc, boolean useDst, boolean useEdge) {
    this.useSrc = useSrc;
    this.useDst = useDst;
    this.useEdge = useEdge;
  }

  /**
   * None of the triplet fields are exposed.
   */
  public static final TripletFields None = new TripletFields(false, false, false);

  /**
   * Expose only the edge field and not the source or destination field.
   */
  public static final TripletFields EdgeOnly = new TripletFields(false, false, true);

  /**
   * Expose the source and edge fields but not the destination field. (Same as Src)
   */
  public static final TripletFields Src = new TripletFields(true, false, true);

  /**
   * Expose the destination and edge fields but not the source field. (Same as Dst)
   */
  public static final TripletFields Dst = new TripletFields(false, true, true);

  /**
   * Expose all the fields (source, edge, and destination).
   */
  public static final TripletFields All = new TripletFields(true, true, true);
}
