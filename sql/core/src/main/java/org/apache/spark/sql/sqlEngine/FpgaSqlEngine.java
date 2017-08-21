package org.apache.spark.sql.sqlEngine;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.List;
import java.util.LinkedList;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.net.InetSocketAddress;
import java.util.Scanner;
import java.lang.Object;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class FpgaSqlEngine {

    private static final Logger logger = LoggerFactory.getLogger(FpgaSqlEngine.class);

    private static Lock lock;
    private static int initFlag = 0;

    static private native ByteBuffer sqlEngineGetBuf(int size);
    static private native void sqlEnginePutBuf(ByteBuffer buf);

    static private native ByteBuffer sqlEngineRun(ByteBuffer buf, int rowCount);

    static void init() {
        if(1 != initFlag) {
            initFlag = 1;
            lock = new ReentrantLock(true);
            logger.warn("WQF: initalizing FPGA lock\n");
        }
    }

    public static ByteBuffer getBuf(int size) {
      init();
      logger.warn("WQF: invoking getBuf");
      return sqlEngineGetBuf(size);
    }

    public static void putBuf(ByteBuffer buf) {
      logger.warn("WQF: invoking putBuf");
      init();
      sqlEnginePutBuf(buf);
    }

    public static ByteBuffer project(ByteBuffer buf, int rowCount) {
      logger.warn("WQF: invoking project");
      init();
      buf.limit(rowCount*768);
      ByteBuffer buffer = sqlEngineRun(buf, rowCount);
      return buffer;
    }
}
