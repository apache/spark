#include "PersistentMemoryPool.h"
#include <string>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <fstream>

PMPool::PMPool(const char* dev, int maxStage, int maxMap, long size):
    maxStage(maxStage),
    maxMap(maxMap),
    stop(false),
    dev(dev),
    worker(&PMPool::process, this) {

  const char *pool_layout_name = "pmem_spark_shuffle";
  cout << "PMPOOL is " << dev << endl;
  // if this is a fsdax device
  // we need to create 
  // if this is a devdax device

  pmpool = pmemobj_open(dev, pool_layout_name);
  if (pmpool == NULL) {
      pmpool = pmemobj_create(dev, pool_layout_name, size, S_IRUSR | S_IWUSR);
  }
  if (pmpool == NULL) {
      cerr << "Failed to create pool, kill process, errmsg: " << pmemobj_errormsg() << endl; 
      exit(-1);
  }

  stageArrayRoot = POBJ_ROOT(pmpool, struct StageArrayRoot);
}

PMPool::~PMPool() {
    while(request_queue.size() > 0) {
        fprintf(stderr, "%s request queue size is %d\n", dev, request_queue.size());
        sleep(1);
    }
    fprintf(stderr, "%s request queue size is %d\n", dev, request_queue.size());
    stop = true;
    worker.join();
    pmemobj_close(pmpool);
}

long PMPool::getRootAddr() {
    return (long)pmpool;
}

void PMPool::process() {
    Request *cur_req;
    while(!stop) {
        cur_req = (Request*)request_queue.dequeue();
        if (cur_req != nullptr) {
            cur_req->exec();
        }
    }
}

long PMPool::setMapPartition(
        int partitionNum,
        int stageId, 
        int mapId, 
        int partitionId,
        long size,
        char* data,
        bool clean,
        int numMaps) {
    WriteRequest write_request(this, maxStage, numMaps, partitionNum, stageId, 0, mapId, partitionId, size, data, clean);
    request_queue.enqueue((void*)&write_request);
    return write_request.getResult();
}

long PMPool::setReducePartition(
        int partitionNum,
        int stageId, 
        int partitionId,
        long size,
        char* data,
        bool clean,
        int numMaps) {
    WriteRequest write_request(this, maxStage, 1, partitionNum, stageId, 1, 0, partitionId, size, data, clean);
    request_queue.enqueue((void*)&write_request);
    
    return write_request.getResult();
}

long PMPool::getMapPartition(
        MemoryBlock* mb,
        int stageId,
        int mapId,
        int partitionId ) {
    ReadRequest read_request(this, mb, stageId, 0, mapId, partitionId);
    read_request.exec();
    return read_request.getResult();
}
    
long PMPool::getReducePartition(
        MemoryBlock* mb,
        int stageId,
        int mapId,
        int partitionId ) {
    ReadRequest read_request(this, mb, stageId, 1, mapId, partitionId);
    read_request.exec();
    read_request.getResult();
}

long PMPool::getMapPartitionBlockInfo(BlockInfo *blockInfo, int stageId, int mapId, int partitionId) {
    MetaRequest meta_request(this, blockInfo, stageId, 0, mapId, partitionId);
    meta_request.exec();
    return meta_request.getResult();
}

long PMPool::getReducePartitionBlockInfo(BlockInfo *blockInfo, int stageId, int mapId, int partitionId) {
    MetaRequest meta_request(this, blockInfo, stageId, 1, mapId, partitionId);
    meta_request.exec();
    return meta_request.getResult();
}

long PMPool::getMapPartitionSize(int stageId, int mapId, int partitionId) {
    SizeRequest size_request(this, stageId, 0, mapId, partitionId);
    size_request.exec();
    return size_request.getResult();
}

long PMPool::getReducePartitionSize(int stageId, int mapId, int partitionId) {
    SizeRequest size_request(this, stageId, 1, mapId, partitionId);
    size_request.exec();
    return size_request.getResult();
}

long PMPool::deleteMapPartition(int stageId, int mapId, int partitionId) {
    DeleteRequest delete_request(this, stageId, 0, mapId, partitionId);
    request_queue.enqueue((void*)&delete_request);
    return delete_request.getResult();
}

long PMPool::deleteReducePartition(int stageId, int mapId, int partitionId) {
    DeleteRequest delete_request(this, stageId, 1, mapId, partitionId);
    request_queue.enqueue((void*)&delete_request);
    return delete_request.getResult();
}

