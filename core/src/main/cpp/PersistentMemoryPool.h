#ifndef PMPOOL_H
#define PMPOOL_H

#include <thread>
#include "WorkQueue.h"
#include "Request.h"

/* This class is used to make PM as a huge pool
 * All shuffle files will be stored in the same pool
 * Default Maximun shuffle file will set to 1000
 * Then follows shuffle block array
 *
 * Data Structure
 * =============================================
 * ===stage_array[0]===
 * 0        shuffle_array[0].address  # contains all maps in this stage
 * 1        shuffle_array[1].address
 * ... ...
 * stage_id: 1000     shuffle_array[999].address
 * ===shuffle_array[0]===
 * 0      shuffle_block_array[0]   # contains all partitions in the shuffle map
 * 1      shuffle_block_array[1]
 * ... ...
 * ===shuffle_block[0]===
 * 0      partition[0].address, partition[0].size
 * 1      partition[1].address, partition[1].size
 * ... ...
 * partition[0]: 2MB block -> 2MB Block -> ...
 * partition[1]
 * ... ...
 * =============================================
 * */

using namespace std;

class PMPool {
public:
    PMEMobjpool *pmpool;

    std::thread worker;
    WorkQueue<void*> request_queue;
    bool stop;

    TOID(struct StageArrayRoot) stageArrayRoot;
    int maxStage;
    int maxMap;
    const char* dev;

    PMPool(const char* dev, int maxStage, int maxMap, long size);
    ~PMPool();
    long getRootAddr();

    long setMapPartition(int partitionNum, int stageId, int mapId, int partitionId, long size, char* data, bool clean, int numMaps);
    long setReducePartition(int partitionNum, int stageId, int partitionId, long size, char* data, bool clean, int numMaps);

    long getMapPartition(MemoryBlock* mb, int stageId, int mapId, int partitionId);
    long getReducePartition(MemoryBlock* mb, int stageId, int mapId, int partitionId);

    long getMapPartitionBlockInfo(BlockInfo *block_info, int stageId, int mapId, int partitionId);
    long getReducePartitionBlockInfo(BlockInfo *block_info, int stageId, int mapId, int partitionId);

    long getMapPartitionSize(int stageId, int mapId, int partitionId);
    long getReducePartitionSize(int stageId, int mapId, int partitionId);

    long deleteMapPartition(int stageId, int mapId, int partitionId);
    long deleteReducePartition(int stageId, int mapId, int partitionId);
private:
    void process();
};

#endif
