package org.apache.spark.sql.hive.execution;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.*;

/**
 * UDF that returns a nested list of maps that uses a string as its key and a list of ints as its
 * values.
 */
public class UDFToListMapStringListInt extends UDF {
    public List<Map<String, List<Integer>>> evaluate(Object o) {
        final Map<String, List<Integer>> map = new HashMap<>();
        map.put("a", Arrays.asList(1, 2));
        map.put("b", Arrays.asList(3, 4));
        return Collections.singletonList(map);
    }
}
