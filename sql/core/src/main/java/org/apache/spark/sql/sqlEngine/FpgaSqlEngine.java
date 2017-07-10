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

public class FpgaSqlEngine {

private static final Logger logger = LoggerFactory.getLogger(FpgaSqlEngine.class);


//static private native int sqlEngineInit(Logger logger);

static private native ByteBuffer sqlEngineGetBuf(int size);
static private native void sqlEnginePutBuf(ByteBuffer buf);

static private native ByteBuffer sqlEngineRun(ByteBuffer buf, int rowCount);

/*
public static int init() {
  logger.warn("### loading sql engine library ...");
  logger.warn(System.getProperty("java.library.path"));

  System.loadLibrary("sqlengine");

  return sqlEngineInit(logger);
}
*/

public static ByteBuffer getBuf(int size) {
  logger.warn("WQF: invoking getBuf");
  return sqlEngineGetBuf(size);
}

public static void putBuf(ByteBuffer buf) {
  logger.warn("WQF: invoking putBuf");
  sqlEnginePutBuf(buf);
}

public static ByteBuffer project(ByteBuffer buf, int rowCount) {
  logger.warn("WQF: invoking project");
  return buf;
//  return sqlEngineRun(buf, rowCount);
}


}
