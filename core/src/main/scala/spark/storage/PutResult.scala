package spark.storage

import java.nio.ByteBuffer

/**
 * Result of adding a block into a BlockStore. Contains its estimated size, and possibly the
 * values put if the caller asked for them to be returned (e.g. for chaining replication)
 */
private[spark] case class PutResult(size: Long, data: Either[Iterator[_], ByteBuffer])
