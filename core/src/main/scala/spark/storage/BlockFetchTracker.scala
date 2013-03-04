package spark.storage

private[spark] trait BlockFetchTracker {
    def totalBlocks : Int
    def numLocalBlocks: Int
    def numRemoteBlocks: Int
    def remoteFetchTime : Long
    def remoteFetchWaitTime: Long
    def remoteBytesRead : Long
}
