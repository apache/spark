package spark.compress.lzf;

public class LZF {
  private static boolean loaded;

  static {
    try {
      System.loadLibrary("spark_native");
      loaded = true;
    } catch(Throwable t) {
      System.out.println("Failed to load native LZF library: " + t.toString());
      loaded = false;
    }
  }

  public static boolean isLoaded() {
    return loaded;
  }

  public static native int compress(
      byte[] in, int inOff, int inLen,
      byte[] out, int outOff, int outLen);

  public static native int decompress(
      byte[] in, int inOff, int inLen,
      byte[] out, int outOff, int outLen);
}
