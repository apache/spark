package spark.network.netty;


public interface PathResolver {
  /**
   * Get the absolute path of the file
   *
   * @param fileId
   * @return the absolute path of file
   */
  public String getAbsolutePath(String fileId);
}
