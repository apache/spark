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

package org.apache.spark.network.shuffledb;

import org.iq80.leveldb.DBIterator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class LevelDB implements DB {
    private final org.iq80.leveldb.DB db;

    public LevelDB(org.iq80.leveldb.DB db) {
        this.db = db;
    }

    @Override
    public void put(byte[] key, byte[] value) throws RuntimeException {
        db.put(key, value);
    }

    @Override
    public byte[] get(byte[] key) throws RuntimeException {
       return db.get(key);
    }

    @Override
    public void delete(byte[] key) throws RuntimeException {
        db.delete(key);
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    @Override
    public Map<String, byte[]> readKVToMap(String prefix) {
        Map<String, byte[]> map = new HashMap<>();
        DBIterator itr = db.iterator();
        itr.seek(prefix.getBytes(StandardCharsets.UTF_8));
        while (itr.hasNext()) {
            Map.Entry<byte[], byte[]> e = itr.next();
            String key = new String(e.getKey(), StandardCharsets.UTF_8);
            if (!key.startsWith(prefix)) {
                break;
            }
            map.put(key, e.getValue());
        }
        return map;
    }
}
