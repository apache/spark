#ifndef PMPOOL_REQUEST_H
#define PMPOOL_REQUEST_H

#include <mutex>
#include <condition_variable>
#include <libpmemobj.h>

#define TOID_ARRAY_TYPE(x) TOID(x)
#define TOID_ARRAY(x) TOID_ARRAY_TYPE(TOID(x))
#define TYPENUM 2

POBJ_LAYOUT_BEGIN(PersistentMemoryStruct);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct StageArrayRoot);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct StageArray);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct StageArrayItem);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct TypeArray);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct TypeArrayItem);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct MapArray);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct MapArrayItem);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct PartitionArray);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct PartitionArrayItem);
POBJ_LAYOUT_TOID(PersistentMemoryStruct, struct PartitionBlock);
POBJ_LAYOUT_END(PersistentMemoryStruct);

struct StageArrayRoot {
    TOID(struct StageArray) stageArray;
};

struct StageArray {
    int size;
    TOID(struct StageArray) nextStageArray;
    TOID(struct StageArrayItem) items[];
};

struct StageArrayItem {
  TOID(struct TypeArray) typeArray;
};

struct TypeArray {
    int size;
    TOID(struct TypeArrayItem) items[];
};

struct TypeArrayItem {
    TOID(struct MapArray) mapArray;
};

struct MapArray {
    int size;
    TOID(struct MapArrayItem) items[];
};

struct MapArrayItem {
    TOID(struct PartitionArray) partitionArray;
};

struct PartitionArray {
    int size;
    int numPartitions;
    TOID(struct PartitionArray) nextPartitionArray;
    TOID(struct PartitionArrayItem) items[];
};

struct PartitionArrayItem {
    TOID(struct PartitionBlock) first_block;
    TOID(struct PartitionBlock) last_block;
    long partition_size;
    int numBlocks;
};

struct PartitionBlock {
    TOID(struct PartitionBlock) next_block;
    long data_size;
    PMEMoid data;
};

struct MemoryBlock {
    char* buf;
    int len;
    MemoryBlock() {
    }

    ~MemoryBlock() {
        delete[] buf;
    }
};

struct BlockInfo {
    long* data;
    BlockInfo() {
    }

    ~BlockInfo() {
        delete data;
    }
};

using namespace std;
class PMPool;
class Request {
public:
    Request(PMPool* pmpool_ptr, int stageId, int typeId, int mapId, int partitionId):
      pmpool_ptr(pmpool_ptr),
      stageId(stageId),
      typeId(typeId),
      mapId(mapId),
      partitionId(partitionId),
      processed(false),
      committed(false),
      lck(mtx), block_lck(block_mtx) {
      }
    ~Request(){}
    virtual void exec() = 0;
    virtual long getResult() = 0;
protected:
    // add lock to make this request blocked
    std::mutex mtx;
    std::condition_variable cv;
    bool processed;
    std::unique_lock<std::mutex> lck;

    // add lock to make func blocked
    std::mutex block_mtx;
    std::condition_variable block_cv;
    bool committed;
    std::unique_lock<std::mutex> block_lck;

    int stageId;
    int typeId;
    int mapId;
    int partitionId;
    PMPool* pmpool_ptr;

    TOID(struct PartitionArrayItem) getOrCreatePartitionBlock(int maxStage, int maxMap, int partitionNum);
    TOID(struct PartitionArrayItem) getPartitionBlock();
};

class WriteRequest : Request {
public:
    char* data_addr; //Pmem Block Addr
    WriteRequest(PMPool* pmpool_ptr,
        int maxStage,
        int maxMap,
        int partitionNum,
        int stageId, 
        int typeId,
        int mapId, 
        int partitionId,
        long size,
        char* data,
        bool set_clean):
    Request(pmpool_ptr, stageId, typeId, mapId, partitionId),
    maxStage(maxStage),
    maxMap(maxMap),
    partitionNum(partitionNum),
    size(size),
    data(data),
    data_addr(nullptr),
    set_clean(set_clean){
    }
    ~WriteRequest(){}
    void exec();
    long getResult();
private:
    int maxStage;
    int maxMap;
    int partitionNum;

    long size;
    char *data;
    bool set_clean;

    void setPartition();
};

class ReadRequest : Request {
public:
    ReadRequest(PMPool* pmpool_ptr,
        MemoryBlock *mb,
        int stageId, 
        int typeId,
        int mapId, 
        int partitionId):
    Request(pmpool_ptr, stageId, typeId, mapId, partitionId), mb(mb){
    }
    ~ReadRequest(){}
    void exec();
    long getResult();
private:
    MemoryBlock *mb;
    long data_length = -1;
    void getPartition();
};

class MetaRequest : Request {
public:
    MetaRequest(PMPool* pmpool_ptr,
        BlockInfo* block_info,
        int stageId, 
        int typeId,
        int mapId, 
        int partitionId):
    Request(pmpool_ptr, stageId, typeId, mapId, partitionId), block_info(block_info){
    }
    ~MetaRequest(){}
    void exec();
    long getResult();
private:
    BlockInfo* block_info;
    long array_length = -1;
    void getPartitionBlockInfo();
};

class SizeRequest: Request {
public:
    SizeRequest(PMPool* pmpool_ptr,
        int stageId, 
        int typeId,
        int mapId, 
        int partitionId):
    Request(pmpool_ptr, stageId, typeId, mapId, partitionId){
    }
    ~SizeRequest(){}
    void exec();
    long getResult();
private:
    long data_length = -1;
    void getPartitionSize();
};

class DeleteRequest: Request {
public:
    DeleteRequest(PMPool* pmpool_ptr,
        int stageId, 
        int typeId,
        int mapId, 
        int partitionId):
    Request(pmpool_ptr, stageId, typeId, mapId, partitionId){
    }
    ~DeleteRequest(){}
    void exec();
    long getResult();
private:
    long ret = -1;
    void deletePartition();
};

#endif
