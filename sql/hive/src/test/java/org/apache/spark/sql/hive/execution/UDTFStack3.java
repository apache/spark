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

package org.apache.spark.sql.hive.execution;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

/**
 * Takes a row of size k of data and splits it into n rows of data.  For
 * example, if n is 3 then the rest of the arguments are split in order into 3
 * rows, each of which has k/3 columns in it (the first emitted row has the
 * first k/3, the second has the second, etc).  If n does not divide k then the
 * remaining columns are padded with NULLs.
 */
@Description(
        name = "stack",
        value = "_FUNC_(n, cols...) - turns k columns into n rows of size k/n each"
)
public class UDTFStack3 extends GenericUDTF {

    @Override
    public void close() throws HiveException {
    }

    private transient List<ObjectInspector> argOIs = new ArrayList<ObjectInspector>();
    private transient Object[] forwardObj = null;
    private transient ArrayList<ReturnObjectInspectorResolver> returnOIResolvers =
            new ArrayList<ReturnObjectInspectorResolver>();
    IntWritable numRows = null;
    Integer numCols = null;

    public StructObjectInspector initialize2(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length < 2)  {
            throw new UDFArgumentException("STACK() expects at least two arguments.");
        }
        if (!(args[0] instanceof ConstantObjectInspector)) {
            throw new UDFArgumentException(
                    "The first argument to STACK() must be a constant integer (got " +
                            args[0].getTypeName() + " instead).");
        }
        numRows = (IntWritable)
                ((ConstantObjectInspector)args[0]).getWritableConstantValue();

        if (numRows == null || numRows.get() < 1) {
            throw new UDFArgumentException(
                    "STACK() expects its first argument to be >= 1.");
        }

        // Divide and round up.
        numCols = (args.length - 1 + numRows.get() - 1) / numRows.get();

        for (int jj = 0; jj < numCols; ++jj) {
            returnOIResolvers.add(new ReturnObjectInspectorResolver());
            for (int ii = 0; ii < numRows.get(); ++ii) {
                int index = ii * numCols + jj + 1;
                if (index < args.length &&
                        !returnOIResolvers.get(jj).update(args[index])) {
                    throw new UDFArgumentException(
                            "Argument " + (jj + 1) + "'s type (" +
                                    args[jj + 1].getTypeName() + ") should be equal to argument " +
                                    index + "'s type (" + args[index].getTypeName() + ")");
                }
            }
        }

        forwardObj = new Object[numCols];
        for (int ii = 0; ii < args.length; ++ii) {
            argOIs.add(args[ii]);
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        for (int ii = 0; ii < numCols; ++ii) {
            fieldNames.add("col" + ii);
            fieldOIs.add(returnOIResolvers.get(ii).get());
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(
                fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args)
            throws HiveException, UDFArgumentException {
        for (int ii = 0; ii < numRows.get(); ++ii) {
            for (int jj = 0; jj < numCols; ++jj) {
                int index = ii * numCols + jj + 1;
                if (index < args.length) {
                    forwardObj[jj] =
                            returnOIResolvers.get(jj).convertIfNecessary(args[index], argOIs.get(index));
                } else {
                    forwardObj[ii] = null;
                }
            }
            forward(forwardObj);
        }
    }

    @Override
    public String toString() {
        return "stack";
    }
}