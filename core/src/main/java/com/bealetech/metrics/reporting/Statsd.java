package com.bealetech.metrics.reporting;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A client to a StatsD server.
 */
public class Statsd implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Statsd.class);

    private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");

    public static enum StatType { COUNTER, TIMER, GAUGE }

    private final String host;
    private final int port;

    private boolean prependNewline = false;

    private ByteArrayOutputStream outputData;
    private DatagramSocket datagramSocket;
    private Writer writer;

    public Statsd(String host, int port) {
        this.host = host;
        this.port = port;

        outputData = new ByteArrayOutputStream();
    }

    public void connect() throws IllegalStateException, SocketException {
        if(datagramSocket != null) {
            throw new IllegalStateException("Already connected");
        }

        prependNewline = false;

        datagramSocket = new DatagramSocket();

        outputData.reset();
        this.writer = new BufferedWriter(new OutputStreamWriter(outputData));
    }

    public void send(String name, String value, StatType statType) throws IOException {
        String statTypeStr = "";
        switch (statType) {
            case COUNTER:
                statTypeStr = "c";
                break;
            case GAUGE:
                statTypeStr = "g";
                break;
            case TIMER:
                statTypeStr = "ms";
                break;
        }
        name = sanitizeString(name);
        String tags = null;
        List<String> parts = new ArrayList<String>(Arrays.asList(name.split("\\.")));
        String prefix = parts.remove(0);
        String source = parts.remove(0);
        if (source.equals("executor")) { // "spark.executor.0.filesystem.file.largeRead_ops" (ExecutorSource)
            String executorId = parts.remove(0);
            tags = String.format("#executor:%s", executorId);
            while (parts.size() > 3) {
                parts.remove(parts.size() -1);
            }
            name = String.format("%s.%s.%s", prefix, source, StringUtils.join(parts, "_"));
        } else if (source.equals("application")) { // "spark.application.Apriori.1394489355680.runtime_ms" (ApplicationSource)
            String applicationName = parts.remove(0);
            String currentTime = parts.remove(0);
            String metricName = parts.remove(0);
            tags = String.format("#application:%s", applicationName);
            name = String.format("%s.%s.", prefix, source) + metricName;
        } else {
            String realSource = parts.remove(0);
            // "spark.OrdersModel.DAGScheduler.stage.failedStages" (DAGSchedulerSource)
            // "spark.OrdersModel.BlockManager.memory.maxMem_MB" (BlockManagerSource)
            if (realSource.equals("DAGScheduler") || realSource.equals("BlockManager")) {
                tags = String.format("#application:%s", source);
                name = String.format("%s.application.%s.", prefix, realSource) + String.format("%s_%s", parts.toArray());
            }
        }

        try {
            if (prependNewline) {
                writer.write("\n");
            }
            writer.write(name);
            writer.write(":");
            writer.write(value);
            writer.write("|");
            writer.write(statTypeStr);
            if (tags != null) {
              writer.write("|");
              writer.write(tags);
            }
            prependNewline = true;
            writer.flush();
        } catch (IOException e) {
            logger.error("Error sending to Statsd:", e);
        }
    }

    @Override
    public void close() throws IOException {
        DatagramPacket packet = newPacket(outputData);

        packet.setData(outputData.toByteArray());
        datagramSocket.send(packet);

        if(datagramSocket != null) {
            datagramSocket.close();
        }
        this.datagramSocket = null;
        this.writer = null;
    }

    private String sanitizeString(String s) {
        return WHITESPACE.matcher(s).replaceAll("-");
    }

    private DatagramPacket newPacket(ByteArrayOutputStream out) {
        byte[] dataBuffer;

        if (out != null) {
            dataBuffer = out.toByteArray();
        }
        else {
            dataBuffer = new byte[8192];
        }

        try {
            return new DatagramPacket(dataBuffer, dataBuffer.length, InetAddress.getByName(this.host), this.port);
        } catch (Exception e) {
            return null;
        }
    }
}
