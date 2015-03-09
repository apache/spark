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

    private static final String DRIVER = "driver";
    private static final String EXECUTOR = "executor";

    private static final String DRIVER_MATCH = ".driver.";
    private static final String EXECUTOR_MATCH = ".executor.";

    public static enum StatType { COUNTER, TIMER, GAUGE }

    private final String host;
    private final int port;

    private String prefix = "spark";
    private String appPrefix = "spark.app-";
    private String yarnAppPrefix = "spark.application_";

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

    public void setNamePrefix(String namePrefix) {
        prefix = namePrefix;
        appPrefix = namePrefix + ".app-";
        yarnAppPrefix = namePrefix + ".application_";
    }

    private String buildMetricName(String rawName) throws IllegalArgumentException {
        rawName = WHITESPACE.matcher(rawName).replaceAll("-");

        // Non-yarn worker metrics
        if (rawName.startsWith(appPrefix)) {
            String[] parts = rawName.split("\\.");
            if (parts.length < 5) {
                throw new IllegalArgumentException("A spark app metric name must contain at least 4 parts: " + rawName);
            }

            StringBuilder stringBuilder = new StringBuilder(prefix);
            if (DRIVER.equals(parts[2])) {
                // e.g. spark.app-20141209201233-0145.driver.BlockManager.memory.maxMem_MB
                stringBuilder.append(rawName.substring(rawName.indexOf(DRIVER_MATCH)));
            } else if (EXECUTOR.equals(parts[3])) {
                // e.g. spark.app-20141209201027-0139.31.executor.filesystem.file.read_bytes
                stringBuilder.append(rawName.substring(rawName.indexOf(EXECUTOR_MATCH)));
            } else if ("jvm".equals(parts[3])) {
                // spark.app-20141212193256-0012.15.jvm.total.max
                stringBuilder.append(rawName.substring(rawName.indexOf(".jvm.")));
            } else {
                throw new IllegalArgumentException("Unrecognized metric name pattern: " + rawName);
            }

            return stringBuilder.toString();
        } else if (rawName.startsWith(yarnAppPrefix)) {
            String[] parts = rawName.split("\\.");

            StringBuilder stringBuilder = new StringBuilder(prefix);

            if (DRIVER.equals(parts[2])) {
                // e.g. spark.application_1418834509223_0044.driver.jvm.non-heap.used
                stringBuilder.append(rawName.substring(rawName.indexOf(DRIVER_MATCH)));
            } else if (EXECUTOR.equals(parts[2])) {
                stringBuilder.append(rawName.substring(rawName.indexOf(EXECUTOR_MATCH)));
            } else if ("".equals(parts[2])) {
                stringBuilder.append(rawName.substring(rawName.indexOf("..")));
            } else {
                throw new IllegalArgumentException("Unrecognized metric name pattern: " + rawName);
            }

            return stringBuilder.toString();
        }

        return rawName;
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

        String tags = null; // TODO: Would be nice to get the job name and job user as tags

        try {
            name = buildMetricName(name);
        } catch (IllegalArgumentException e) {
            logger.error("Error sending to Statsd:", e);
            return; // Drop metrics that we can't process so we don't push metrics with app names (e.g. 20141209201233-0145)
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
