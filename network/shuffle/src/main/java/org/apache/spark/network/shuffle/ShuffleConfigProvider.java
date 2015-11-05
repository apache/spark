package org.apache.spark.network.shuffle;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.spark.network.util.ConfigProvider;
import org.apache.spark.network.util.TransportConf;

/**
 * Use the corresponding shuffle configuration when retrieving a network configuration. If this
 * shuffle configuration doesn't exist, use the original network configuration instead.
 */
public class ShuffleConfigProvider extends ConfigProvider {

  private static final String SPARK_SHUFFLE_IO_MODE_KEY = "spark.shuffle.io.mode";
  private static final String SPARK_SHUFFLE_IO_PREFERDIRECTBUFS_KEY =
    "spark.shuffle.io.preferDirectBufs";
  private static final String SPARK_SHUFFLE_IO_CONNECTIONTIMEOUT_KEY =
      "spark.shuffle.io.connectionTimeout";
  private static final String SPARK_SHUFFLE_IO_BACKLOG_KEY = "spark.shuffle.io.backLog";
  public static final String SPARK_SHUFFLE_IO_NUMCONNECTIONSPERPEER_KEY =
      "spark.shuffle.io.numConnectionsPerPeer";
  private static final String SPARK_SHUFFLE_IO_SERVERTHREADS_KEY = "spark.shuffle.io.serverThreads";
  private static final String SPARK_SHUFFLE_IO_CLIENTTHREADS_KEY = "spark.shuffle.io.clientThreads";
  private static final String SPARK_SHUFFLE_IO_RECEIVEBUFFER_KEY = "spark.shuffle.io.receiveBuffer";
  private static final String SPARK_SHUFFLE_IO_SENDBUFFER_KEY = "spark.shuffle.io.sendBuffer";
  private static final String SPARK_SHUFFLE_SASL_TIMEOUT_KEY = "spark.shuffle.sasl.timeout";
  public static final String SPARK_SHUFFLE_IO_MAXRETRIES_KEY = "spark.shuffle.io.maxRetries";
  public static final String SPARK_SHUFFLE_IO_RETRYWAIT_KEY = "spark.shuffle.io.retryWait";

  private static final Map<String, String> NETWORK_TO_SHUFFLE_CONF_MAPPINGS =
    new HashMap<String, String>() {
      {
        put(TransportConf.SPARK_NETWORK_IO_MODE_KEY, SPARK_SHUFFLE_IO_MODE_KEY);
        put(TransportConf.SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY,
          SPARK_SHUFFLE_IO_PREFERDIRECTBUFS_KEY);
        put(TransportConf.SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY,
          SPARK_SHUFFLE_IO_CONNECTIONTIMEOUT_KEY);
        put(TransportConf.SPARK_NETWORK_IO_BACKLOG_KEY, SPARK_SHUFFLE_IO_BACKLOG_KEY);
        put(TransportConf.SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY,
            SPARK_SHUFFLE_IO_NUMCONNECTIONSPERPEER_KEY);
        put(TransportConf.SPARK_NETWORK_IO_SERVERTHREADS_KEY, SPARK_SHUFFLE_IO_SERVERTHREADS_KEY);
        put(TransportConf.SPARK_NETWORK_IO_CLIENTTHREADS_KEY, SPARK_SHUFFLE_IO_CLIENTTHREADS_KEY);
        put(TransportConf.SPARK_NETWORK_IO_RECEIVEBUFFER_KEY, SPARK_SHUFFLE_IO_RECEIVEBUFFER_KEY);
        put(TransportConf.SPARK_NETWORK_IO_SENDBUFFER_KEY, SPARK_SHUFFLE_IO_SENDBUFFER_KEY);
        put(TransportConf.SPARK_NETWORK_SASL_TIMEOUT_KEY, SPARK_SHUFFLE_SASL_TIMEOUT_KEY);
        put(TransportConf.SPARK_NETWORK_IO_MAXRETRIES_KEY, SPARK_SHUFFLE_IO_MAXRETRIES_KEY);
        put(TransportConf.SPARK_NETWORK_IO_RETRYWAIT_KEY, SPARK_SHUFFLE_IO_RETRYWAIT_KEY);
      }
    };

  private ConfigProvider configProvider;

  public ShuffleConfigProvider(ConfigProvider configProvider) {
    this.configProvider = configProvider;
  }

  @Override
  public String get(String name) {
    String shuffleConfKey = NETWORK_TO_SHUFFLE_CONF_MAPPINGS.get(name);
    if (shuffleConfKey != null) {
      try {
        return configProvider.get(shuffleConfKey);
      } catch (NoSuchElementException e) {
        return configProvider.get(name);
      }
    } else {
      return configProvider.get(name);
    }
  }
}
