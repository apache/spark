package org.apache.spark.network.remoteshuffle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

/***
 * This class creates shuffle file and writes shuffle record to file.
 */
public class PartitionWriter implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(PartitionWriter.class);

  private static final String FILE_NAME = "shuffle.data";

  private final String filePath;

  private FileOutputStream fileStream;

  public PartitionWriter(String partitionRootDir) {
    (new File(partitionRootDir)).mkdirs();
    this.filePath = Paths.get(partitionRootDir, FILE_NAME).toString();
  }

  public synchronized void writeTaskData(ByteBuffer data) {
    openFileIfNecessary();

    byte[] bytes = new byte[data.remaining()];
    data.get(bytes);
    try {
      fileStream.write(bytes);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to write file: %s", filePath), e);
    }
  }

  public synchronized void flush() {
    flushFileStream();

    // TODO it may have performance issue closing file on each flush operation.
    // Need to find another way to close file properly to improve performance.
    closeFileStream();
  }

  @Override
  public synchronized void close() {
    closeFileStream();
  }

  private void openFileIfNecessary() {
    if (fileStream == null) {
      try {
        fileStream = new FileOutputStream(filePath, true);
      } catch (FileNotFoundException e) {
        throw new RuntimeException(String.format("Failed to open file: %s", filePath), e);
      }
    }
  }

  private void flushFileStream() {
    if (fileStream != null) {
      try {
        fileStream.flush();
        logger.debug("Flushed file {}", filePath);
      } catch (IOException e) {
        throw new RuntimeException(String.format("Failed to flush file: %s", filePath), e);
      }
    }
  }

  private void closeFileStream() {
    if (fileStream != null) {
      try {
        fileStream.close();
      } catch (IOException e) {
        throw new RuntimeException(String.format("Failed to close file: %s", filePath), e);
      }
      fileStream = null;
    }
  }
}
