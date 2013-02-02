package spark;

import java.io.File;

/**
 * Resolves paths to files added through `SparkContext.addFile()`.
 */
public class SparkFiles {

  private SparkFiles() {}

  /**
   * Get the absolute path of a file added through `SparkContext.addFile()`.
   */
  public static String get(String filename) {
    return new File(getRootDirectory(), filename).getAbsolutePath();
  }

  /**
   * Get the root directory that contains files added through `SparkContext.addFile()`.
   */
  public static String getRootDirectory() {
    return SparkEnv.get().sparkFilesDir();
  }
}
