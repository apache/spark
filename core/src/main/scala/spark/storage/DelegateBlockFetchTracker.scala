package spark.storage

private[spark] trait DelegateBlockFetchTracker extends BlockFetchTracker {
  var delegate : BlockFetchTracker = _
  def setDelegate(d: BlockFetchTracker) {delegate = d}
  def totalBlocks = delegate.totalBlocks
  def numLocalBlocks = delegate.numLocalBlocks
  def numRemoteBlocks = delegate.numRemoteBlocks
  def remoteFetchTime = delegate.remoteFetchTime
  def fetchWaitTime = delegate.fetchWaitTime
  def remoteBytesRead = delegate.remoteBytesRead
}
