package org.apache.spark.network.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.network.shuffledb.LevelDBImpl;
import org.apache.spark.network.shuffledb.LocalDB;
import org.apache.spark.network.shuffledb.StoreVersion;
import org.iq80.leveldb.DB;

import java.io.File;
import java.io.IOException;

public class LocalDBProvider {
    public static LocalDB initLocalDB(File dbFile, StoreVersion version, ObjectMapper mapper)
        throws IOException {
        if (dbFile != null) {
            if (dbFile.getName().endsWith(".ldb")) {
                DB levelDB = LevelDBProvider.initLevelDB(dbFile, version, mapper);
                return levelDB != null ? new LevelDBImpl(levelDB) : null;
            } else {
                return null;
            }
        }
        return null;
    }
}
