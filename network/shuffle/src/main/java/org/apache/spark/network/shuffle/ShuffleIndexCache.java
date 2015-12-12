package org.apache.spark.network.shuffle;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.JavaUtils;

/**
 * Store the offsets of the data blocks in cache.
 * When index cache is not enough, remove firstly used index information.
 */
public class ShuffleIndexCache {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleIndexCache.class);

  private final ConcurrentMap<ShuffleMapId, IndexInformation> indexCache;
  private final LinkedBlockingQueue<ShuffleMapId> queue = new LinkedBlockingQueue<ShuffleMapId>();
  private final int totalMemoryAllowed;
  private AtomicInteger totalMemoryUsed = new AtomicInteger();

  public ShuffleIndexCache(int totalMemoryAllowed) {
    this.indexCache = new ConcurrentHashMap<ShuffleMapId, IndexInformation>();
    this.totalMemoryAllowed = totalMemoryAllowed;
    logger.info("IndexCache created with max memory = {}", totalMemoryAllowed);
  }

  /**
   * Get the index information for the given shuffleId, mapId and reduceId.
   * It reads the index file into cache if it is not already present.
   */
  public ShuffleIndexRecord getIndexInformation(
    ExecutorShuffleInfo executor, int shuffleId, int mapId, int reduceId) throws IOException {
    if (totalMemoryAllowed > 0) {
      ShuffleMapId shuffleMapId = new ShuffleMapId(shuffleId, mapId);
      IndexInformation info = indexCache.get(shuffleMapId);

      if (info == null) {
        info = readIndexFileToCache(executor, shuffleMapId);
      } else {
        synchronized(info) {
          while (isUnderConstruction(info)) {
            try {
              info.wait();
            } catch (InterruptedException e) {
              throw new IOException("Interrupted waiting for construction", e);
            }
          }
        }
      }

      if(info.getLength() == 0 || info.getLength() <= reduceId + 1) {
        throw new IOException("Invalid request " + " shuffleMapId = " + shuffleMapId +
          " reduceId = " + reduceId + " Index Info Length = " + info.getLength() +
            " index file = " + getIndexFile(executor, mapId, reduceId));
      }

      return info.getIndex(reduceId);
    } else {
      return this.readIndexFile(executor, shuffleId, mapId, reduceId);
    }
  }

  public ShuffleIndexRecord readIndexFile(
    ExecutorShuffleInfo executor, int shuffleId, int mapId, int reduceId) throws IOException {
    File indexFile = this.getIndexFile(executor, shuffleId, mapId);

    DataInputStream in = null;
    try {
      in = new DataInputStream(new FileInputStream(indexFile));
      in.skipBytes(reduceId * 8);
      long offset = in.readLong();
      long nextOffset = in.readLong();
      return new ShuffleIndexRecord(offset, nextOffset);
    } finally {
      if (in != null) {
        JavaUtils.closeQuietly(in);
      }
    }
  }

  /**
   * Get the index information from index file and then put index information into cache.
   */
  private IndexInformation readIndexFileToCache(
    ExecutorShuffleInfo executor, ShuffleMapId shuffleMapId) throws IOException {
    IndexInformation info;
    IndexInformation newInd = new IndexInformation();
    if ((info = indexCache.putIfAbsent(shuffleMapId, newInd)) != null) {
      synchronized(info) {
        while (isUnderConstruction(info)) {
          try {
            info.wait();
          } catch (InterruptedException e) {
            throw new IOException("Interrupted waiting for construction", e);
          }
        }
      }
      logger.debug("IndexCache: ShuffleMapId " + shuffleMapId + " found");
      return info;
    }

    logger.debug("IndexCache: ShuffleMapId " + shuffleMapId + " not found") ;
    File indexFile = this.getIndexFile(executor, shuffleMapId.shuffleId, shuffleMapId.mapId);

    LongBuffer tmp = null;
    DataInputStream in = null;
    try {
      int indexSize = (int)indexFile.length();
      in = new DataInputStream(new FileInputStream(indexFile));
      ByteBuffer buf = ByteBuffer.allocate(indexSize);
      int toRead = indexSize;
      int off = 0;
      while (toRead > 0) {
        int ret = in.read(buf.array(), off, toRead);
        if (ret < 0) {
          throw new IOException("Premature EOF from inputStream of file: " + indexFile);
        }
        toRead -= ret;
        off += ret;
      }
      tmp = buf.asLongBuffer();
    } catch (IOException e) {
      ByteBuffer emptyBuf = ByteBuffer.allocate(0);
      tmp = emptyBuf.asLongBuffer();
      indexCache.remove(shuffleMapId);
      throw new IOException("Failed to open file: " + indexFile, e);
    } finally {
      if (in != null) {
        JavaUtils.closeQuietly(in);
      }
      synchronized (newInd) {
        newInd.offsets = tmp;
        newInd.notifyAll();
      }
    }

    queue.add(shuffleMapId);

    if (totalMemoryUsed.addAndGet(newInd.getSize()) > totalMemoryAllowed) {
      freeIndexInformation();
    }
    return newInd;
  }

  /** Whether the index information is under construction*/
  private boolean isUnderConstruction(IndexInformation info) {
    synchronized(info) {
      return (null == info.offsets);
    }
  }

  private File getIndexFile(ExecutorShuffleInfo executor, int shuffleId, int mapId) {
    return ExternalShuffleBlockResolver.getFile(executor.localDirs, executor.subDirsPerLocalDir,
      "shuffle_" + shuffleId + "_" + mapId + "_0.index");
  }

  /**
   * when index cache is not enough, remove first used index information.
   */
  private synchronized void freeIndexInformation() {
    while (totalMemoryUsed.get() > totalMemoryAllowed) {
      ShuffleMapId shuffleMapId = queue.remove();
      IndexInformation index = indexCache.remove(shuffleMapId);
      if (index != null) {
        logger.debug("IndexCache: ShuffleMapId " + shuffleMapId + " 's index are free");
        totalMemoryUsed.addAndGet(-index.getSize());
      }
    }
  }

  /** This method removes the mapId of shuffleId from the cache */
  @VisibleForTesting
  public void removeMap(int shuffleId, int mapId) {
    ShuffleMapId shuffleMapId = new ShuffleMapId(shuffleId, mapId);
    IndexInformation info = indexCache.get(shuffleMapId);
    if (info == null || isUnderConstruction(info)) {
      return;
    }
    info = indexCache.remove(shuffleMapId);
    if (info != null) {
      totalMemoryUsed.addAndGet(-info.getSize());
      if (!queue.remove(shuffleMapId)) {
        logger.warn("ShuffleMapId" + shuffleMapId + " not found in queue!!");
      }
    } else {
      logger.info("ShuffleMapId " + shuffleMapId + " not found in cache");
    }
  }

  private static class IndexInformation {
    LongBuffer offsets;

    public int getLength() {
      return offsets == null ? 0 : offsets.capacity();
    }

    public int getSize() {
      return offsets == null ? 0 : offsets.capacity() * 8;
    }

    public ShuffleIndexRecord getIndex(int reduceId) {
      return new ShuffleIndexRecord(offsets.get(reduceId), offsets.get(reduceId + 1));
    }
  }

  private static class ShuffleMapId {
    private int shuffleId;
    private int mapId;

    private ShuffleMapId(int shuffleId, int mapId) {
      this.shuffleId = shuffleId;
      this.mapId = mapId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;

      ShuffleMapId shuffleMapId = (ShuffleMapId) o;
      return shuffleId == shuffleMapId.shuffleId && mapId == shuffleMapId.mapId;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(shuffleId, mapId);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("shuffleId", shuffleId)
        .add("mapId", mapId)
        .toString();
    }
  }
}


