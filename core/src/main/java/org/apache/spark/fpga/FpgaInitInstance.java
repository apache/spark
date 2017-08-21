package org.apache.spark.fpga;

public class FpgaInitInstance {

    static private native void sqlEngineInit();

    public static void initFpgaInstance() {
        sqlEngineInit();
    }
}
