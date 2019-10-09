package io.radanalytics.operator.historyServer;

import io.radanalytics.types.HistoryServer;
import io.radanalytics.types.SparkCluster;
import io.radanalytics.types.SparkHistoryServer;

public class HistoryServerHelper {

    public static boolean needsVolume(SparkHistoryServer hs) {
        return HistoryServer.Type.sharedVolume.value().equals(hs.getType().value());
    }

    public static boolean needsVolume(SparkCluster cluster) {
        return null != cluster.getHistoryServer() && HistoryServer.Type.sharedVolume.value().equals(cluster.getHistoryServer().getType().value());
    }
}
