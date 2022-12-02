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

package org.apache.spark.sql.types;

import org.apache.spark.sql.catalyst.types.*;

/**
 * To get specific physical data type.
 *
 * @since 3.4.0
 */
public class PhysicalDataTypes {
    /**
     * Gets the PhysicalStringType object.
     */
    public static final PhysicalDataType StringType = PhysicalStringType$.MODULE$;

    /**
     * Gets the PhysicalBinaryType object.
     */
    public static final PhysicalDataType BinaryType = PhysicalBinaryType$.MODULE$;

    /**
     * Gets the PhysicalBooleanType object.
     */
    public static final PhysicalDataType BooleanType = PhysicalBooleanType$.MODULE$;

    /**
     * Gets the PhysicalCalendarIntervalType object.
     */
    public static final PhysicalDataType CalendarIntervalType = PhysicalCalendarIntervalType$.MODULE$;

    /**
     * Gets the PhysicalDoubleType object.
     */
    public static final PhysicalDataType DoubleType = PhysicalDoubleType$.MODULE$;

    /**
     * Gets the PhysicalFloatType object.
     */
    public static final PhysicalDataType FloatType = PhysicalFloatType$.MODULE$;

    /**
     * Gets the PhysicalByteType object.
     */
    public static final PhysicalDataType ByteType = PhysicalByteType$.MODULE$;

    /**
     * Gets the PhysicalIntegerType object.
     */
    public static final PhysicalDataType IntegerType = PhysicalIntegerType$.MODULE$;

    /**
     * Gets the PhysicalLongType object.
     */
    public static final PhysicalDataType LongType = PhysicalLongType$.MODULE$;

    /**
     * Gets the PhysicalShortType object.
     */
    public static final PhysicalDataType ShortType = PhysicalShortType$.MODULE$;

    /**
     * Gets the PhysicalNullType object.
     */
    public static final PhysicalDataType NullType = PhysicalNullType$.MODULE$;
}
