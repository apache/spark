#include "Request.h"
#include "PersistentMemoryPool.h"
#include <string>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>

/******  Request ******/
TOID(struct PartitionArrayItem) Request::getPartitionBlock() {
    if(TOID_IS_NULL(D_RO(pmpool_ptr->stageArrayRoot)->stageArray)){
        fprintf(stderr, "get Partition of %d_%d_%d failed: stageArray none Exists, type id %d.\n", stageId, mapId, partitionId, typeId);
        return TOID_NULL(struct PartitionArrayItem);
    }

    /** get if stageId is greater than default maxStages.**/
    TOID(struct StageArray) stageArray = D_RO(pmpool_ptr->stageArrayRoot)->stageArray;
    int stageArrayIndex = stageId / pmpool_ptr->maxStage;
    int stageItemIndex = stageId % pmpool_ptr->maxStage;
    for (int i = 0; i < stageArrayIndex; i++) {
        if (TOID_IS_NULL(D_RO(stageArray)->nextStageArray)) {
            fprintf(stderr, "get Partition of %d_%d_%d failed: stageArray none Exists, type id %d.\n", stageId, mapId, partitionId, typeId);
            return TOID_NULL(struct PartitionArrayItem);
        }
        stageArray = D_RO(stageArray)->nextStageArray;
    }
    /** get done **/

    TOID(struct StageArrayItem) stageArrayItem = D_RO(stageArray)->items[stageItemIndex];
    if(TOID_IS_NULL(stageArrayItem) || TOID_IS_NULL(D_RO(stageArrayItem)->typeArray)) {
        fprintf(stderr, "get Partition of %d_%d_%d failed: stageArrayItem OR typeArray none Exists, type id %d.\n", stageId, mapId, partitionId, typeId);
        return TOID_NULL(struct PartitionArrayItem);
    }

    TOID(struct TypeArray) typeArray = D_RO(stageArrayItem)->typeArray;
    TOID(struct TypeArrayItem) typeArrayItem = D_RO(D_RO(stageArrayItem)->typeArray)->items[typeId];
    if(TOID_IS_NULL(typeArrayItem) || TOID_IS_NULL(D_RO(typeArrayItem)->mapArray)) {
        fprintf(stderr, "get Partition of %d_%d_%d failed: typeArrayItem OR mapArray none Exists, type id %d.\n", stageId, mapId, partitionId, typeId);
        return TOID_NULL(struct PartitionArrayItem);
    }

    TOID(struct MapArray) mapArray = D_RO(typeArrayItem)->mapArray;
    TOID(struct MapArrayItem) mapArrayItem = D_RO(D_RO(typeArrayItem)->mapArray)->items[mapId];
    if(TOID_IS_NULL(mapArrayItem) || TOID_IS_NULL(D_RO(mapArrayItem)->partitionArray)){
        fprintf(stderr, "get Partition of %d_%d_%d failed: mapArrayItem OR partitionArray none Exists, type id %d.\n", stageId, mapId, partitionId, typeId);
        return TOID_NULL(struct PartitionArrayItem);
    }

    /** Do append and get if stageId is greater than default maxStages.**/
    TOID(struct PartitionArray) partitionArray = D_RO(mapArrayItem)->partitionArray;
    int partitionNum = D_RO(partitionArray)->numPartitions;
    int partitionArrayIndex = partitionId / partitionNum;
    int partitionItemIndex = partitionId % partitionNum;
    for (int i = 0; i < partitionArrayIndex; i++) {
        if (TOID_IS_NULL(D_RO(partitionArray)->nextPartitionArray)) {
            fprintf(stderr, "get Partition of %d_%d_%d failed: partitionArray none Exists, type id %d.\n", stageId, mapId, partitionId, typeId);
            return TOID_NULL(struct PartitionArrayItem);
        }
        partitionArray = D_RO(partitionArray)->nextPartitionArray;
    }
    /** append get done **/

    TOID(struct PartitionArrayItem) partitionArrayItem = D_RO(partitionArray)->items[partitionItemIndex];
    if(TOID_IS_NULL(partitionArrayItem) || TOID_IS_NULL(D_RO(partitionArrayItem)->first_block)) {
        fprintf(stderr, "get Partition of %d_%d_%d failed: partitionArrayItem OR partitionBlock none Exists, type id %d.\n", stageId, mapId, partitionId, typeId);
        return TOID_NULL(struct PartitionArrayItem);
    }

    return partitionArrayItem;
}

TOID(struct PartitionArrayItem) Request::getOrCreatePartitionBlock(int maxStage, int maxMap, int partitionNum) {
        if (TOID_IS_NULL(D_RO(pmpool_ptr->stageArrayRoot)->stageArray)) {
            D_RW(pmpool_ptr->stageArrayRoot)->stageArray = TX_ZALLOC(struct StageArray, sizeof(struct StageArray) + maxStage * sizeof(struct StageArrayItem));
        }
        
        /** Do append and get if stageId is greater than default maxStages.**/
        TX_ADD_FIELD(pmpool_ptr->stageArrayRoot, stageArray);
        TOID(struct StageArray) *stageArray = &(D_RW(pmpool_ptr->stageArrayRoot)->stageArray);
        int stageArrayIndex = stageId / maxStage;
        int stageItemIndex = stageId % maxStage;
        for (int i = 0; i < stageArrayIndex; i++) {
            if (TOID_IS_NULL(D_RO(*stageArray)->nextStageArray)) {
                D_RW(*stageArray)->nextStageArray = TX_ZALLOC(struct StageArray, sizeof(struct StageArray) + maxStage * sizeof(struct StageArrayItem));
            }
            stageArray = &(D_RW(*stageArray)->nextStageArray);
            TX_ADD(*stageArray);
        }
        /** append get done **/

        TOID(struct StageArrayItem) *stageArrayItem = &(D_RW(*stageArray)->items[stageItemIndex]);
        if (TOID_IS_NULL(*stageArrayItem)) {
            *stageArrayItem = TX_ZNEW(struct StageArrayItem);
        }
        TX_ADD(*stageArrayItem);
        if (TOID_IS_NULL(D_RO(*stageArrayItem)->typeArray)) {
            D_RW(*stageArrayItem)->typeArray = TX_ZALLOC(struct TypeArray, sizeof(struct TypeArray) + TYPENUM * sizeof(struct TypeArrayItem));
        }

        TX_ADD_FIELD(*stageArrayItem, typeArray);
        TOID(struct TypeArrayItem) *typeArrayItem = &(D_RW(D_RW(*stageArrayItem)->typeArray)->items[typeId]);
        if (TOID_IS_NULL(*typeArrayItem)) {
            *typeArrayItem = TX_ZNEW(struct TypeArrayItem);
        }
        TX_ADD(*typeArrayItem);
        if (TOID_IS_NULL(D_RO(*typeArrayItem)->mapArray)) {
            D_RW(*typeArrayItem)->mapArray = TX_ZALLOC(struct MapArray, sizeof(struct MapArray) + maxMap * sizeof(struct MapArrayItem));
        }

        TX_ADD_FIELD(*typeArrayItem, mapArray);
        TOID(struct MapArrayItem) *mapArrayItem = &(D_RW(D_RW(*typeArrayItem)->mapArray)->items[mapId]);
        if (TOID_IS_NULL(*mapArrayItem)) {
            *mapArrayItem = TX_ZNEW(struct MapArrayItem);
        }
        TX_ADD(*mapArrayItem);
        if (TOID_IS_NULL(D_RO(*mapArrayItem)->partitionArray)) {
            D_RW(*mapArrayItem)->partitionArray = TX_ZALLOC(struct PartitionArray, sizeof(struct PartitionArray) +  partitionNum * sizeof(struct PartitionArrayItem));
            TX_ADD_FIELD(*mapArrayItem, partitionArray);
            D_RW(D_RW(*mapArrayItem)->partitionArray)->numPartitions = partitionNum; 
        } else {
            TX_ADD_FIELD(*mapArrayItem, partitionArray);
        }
        TOID(struct PartitionArray) *partitionArray = &(D_RW(*mapArrayItem)->partitionArray);

        /** Do append and get if stageId is greater than default maxStages.**/
        int partitionArrayIndex = partitionId / partitionNum;
        int partitionItemIndex = partitionId % partitionNum;

        for (int i = 0; i < partitionArrayIndex; i++) {
            if (TOID_IS_NULL(D_RO(*partitionArray)->nextPartitionArray)) {
                D_RW(*partitionArray)->nextPartitionArray = TX_ZALLOC(struct PartitionArray, sizeof(struct PartitionArray) + partitionNum * sizeof(struct PartitionArrayItem));
                TX_ADD_FIELD(*partitionArray, nextPartitionArray);
                D_RW(D_RW(*partitionArray)->nextPartitionArray)->numPartitions = partitionNum; 
            } else {
                TX_ADD_FIELD(*partitionArray, nextPartitionArray);
            }
            partitionArray = &(D_RW(*partitionArray)->nextPartitionArray);
        }
        /** append get done **/

        TOID(struct PartitionArrayItem) *partitionArrayItem = &(D_RW(*partitionArray)->items[partitionItemIndex]);
        if (TOID_IS_NULL(*partitionArrayItem)) {
            *partitionArrayItem = TX_ZNEW(struct PartitionArrayItem);
        }
        return *partitionArrayItem;
}

/******  WriteRequest ******/
void WriteRequest::exec() {
    setPartition();
}

long WriteRequest::getResult() {
    while (!processed) {
        usleep(5);
    }
    //cv.wait(lck, [&]{return processed;});
    //fprintf(stderr, "get Result for %d_%d_%d\n", stageId, mapId, partitionId);
    return (long)data_addr;
}

void WriteRequest::setPartition() {
    TX_BEGIN(pmpool_ptr->pmpool) {
        TX_ADD(pmpool_ptr->stageArrayRoot);

        TOID(struct PartitionArrayItem) partitionArrayItem = getOrCreatePartitionBlock(maxStage, maxMap, partitionNum);
        TX_ADD(partitionArrayItem);
        TX_ADD_FIELD(partitionArrayItem, partition_size);

        TOID(struct PartitionBlock) *partitionBlock = &(D_RW(partitionArrayItem)->first_block);
        TOID(struct PartitionBlock) *last_partitionBlock = &(D_RW(partitionArrayItem)->last_block);
        if (set_clean == false) {
            // jump to last block
            D_RW(partitionArrayItem)->partition_size += size;
            D_RW(partitionArrayItem)->numBlocks += 1;
            if (!TOID_IS_NULL(*last_partitionBlock)) {
              partitionBlock = &(D_RW(*last_partitionBlock)->next_block);
            }
        } else {
            TOID(struct PartitionBlock) curPartitionBlock = *partitionBlock;
            TOID(struct PartitionBlock) nextPartitionBlock = curPartitionBlock;
    
            //remove original blocks per set_clean is true
            while(!TOID_IS_NULL(curPartitionBlock)) {
                nextPartitionBlock = D_RO(curPartitionBlock)->next_block;
                pmemobj_tx_free(D_RO(curPartitionBlock)->data);
                TX_FREE(curPartitionBlock);
                curPartitionBlock = nextPartitionBlock;
            }

            D_RW(partitionArrayItem)->partition_size = size;
            D_RW(partitionArrayItem)->numBlocks = 1;
        }

        TX_ADD_DIRECT(partitionBlock);
        *partitionBlock = TX_ZALLOC(struct PartitionBlock, 1);
        D_RW(*partitionBlock)->data = pmemobj_tx_zalloc(size, 0);
        
        D_RW(*partitionBlock)->data_size = size;
        D_RW(*partitionBlock)->next_block = TOID_NULL(struct PartitionBlock);
        D_RW(partitionArrayItem)->last_block = *partitionBlock;

        data_addr = (char*)pmemobj_direct(D_RW(*partitionBlock)->data);
        //printf("setPartition data_addr: %p\n", data_addr);
        pmemobj_tx_add_range_direct((const void *)data_addr, size);

        memcpy(data_addr, data, size);
    } TX_ONCOMMIT {
        committed = true;
        block_cv.notify_all();
    } TX_ONABORT {
        fprintf(stderr, "set Partition of %d_%d_%d failed, type is %d, partitionNum is %d, maxStage is %d, maxMap is %d. Error: %s\n", stageId, mapId, partitionId, typeId, partitionNum, maxStage, maxMap, pmemobj_errormsg());
        exit(-1);
    } TX_END

    block_cv.wait(block_lck, [&]{return committed;});
    //fprintf(stderr, "request committed %d_%d_%d\n", stageId, mapId, partitionId);

    processed = true;
    //cv.notify_all();
}

/******  ReadRequest ******/
void ReadRequest::exec() {
    getPartition();
}

long ReadRequest::getResult() {
    return data_length;
}

void ReadRequest::getPartition() {
    //taskset(core_s, core_e); 
    TOID(struct PartitionArrayItem) partitionArrayItem = getPartitionBlock();
    if (TOID_IS_NULL(partitionArrayItem)) return;
    data_length = D_RO(partitionArrayItem)->partition_size;
    mb->buf = new char[data_length]();
    long off = 0;
    TOID(struct PartitionBlock) partitionBlock = D_RO(partitionArrayItem)->first_block;

    char* data_addr;
    while(!TOID_IS_NULL(partitionBlock)) {
        data_addr = (char*)pmemobj_direct(D_RO(partitionBlock)->data);
        //printf("getPartition data_addr: %p\n", data_addr);

        memcpy(mb->buf + off, data_addr, D_RO(partitionBlock)->data_size);
        off += D_RO(partitionBlock)->data_size;
        partitionBlock = D_RO(partitionBlock)->next_block;
    }

    //printf("getPartition length is %d\n", data_length);
}

/******  MetaRequest ******/
void MetaRequest::exec() {
    getPartitionBlockInfo();
}

long MetaRequest::getResult() {
    return array_length;
}

void MetaRequest::getPartitionBlockInfo() {
    TOID(struct PartitionArrayItem) partitionArrayItem = getPartitionBlock();
    if (TOID_IS_NULL(partitionArrayItem)) return;
    TOID(struct PartitionBlock) partitionBlock = D_RO(partitionArrayItem)->first_block;
    if (TOID_IS_NULL(partitionBlock)) return;

    int numBlocks = D_RO(partitionArrayItem)->numBlocks;
    array_length = numBlocks * 2;
    block_info->data = new long[array_length]();
    int i = 0;

    while(!TOID_IS_NULL(partitionBlock)) {
        block_info->data[i++] = (long)pmemobj_direct(D_RO(partitionBlock)->data);
        block_info->data[i++] = D_RO(partitionBlock)->data_size;
        partitionBlock = D_RO(partitionBlock)->next_block;
    }
}

/******  SizeRequest ******/
void SizeRequest::exec() {
    getPartitionSize();
}

long SizeRequest::getResult() {
    return data_length;
}

void SizeRequest::getPartitionSize() {
    TOID(struct PartitionArrayItem) partitionArrayItem = getPartitionBlock();
    if (TOID_IS_NULL(partitionArrayItem)) return;
    data_length = D_RO(partitionArrayItem)->partition_size;
}


/******  DeleteRequest ******/
void DeleteRequest::exec() {
    deletePartition();
}

long DeleteRequest::getResult() {
    while (!processed) {
        usleep(5);
    }
    return ret;
}

void DeleteRequest::deletePartition() {
    TX_BEGIN(pmpool_ptr->pmpool) {
        TOID(struct PartitionArrayItem) partitionArrayItem = getPartitionBlock();
        if (TOID_IS_NULL(partitionArrayItem)) return;
        TOID(struct PartitionBlock) partitionBlock = D_RO(partitionArrayItem)->first_block;
        TOID(struct PartitionBlock) nextPartitionBlock = partitionBlock;
    
        while(!TOID_IS_NULL(partitionBlock)) {
            nextPartitionBlock = D_RO(partitionBlock)->next_block;
            pmemobj_tx_free(D_RO(partitionBlock)->data);
            TX_FREE(partitionBlock);
            partitionBlock = nextPartitionBlock;
        }
        //TX_FREE(partitionArrayItem);
        //*(&partitionArrayItem) = TOID_NULL(struct PartitionArrayItem);
        D_RW(partitionArrayItem)->first_block = TOID_NULL(struct PartitionBlock);
        D_RW(partitionArrayItem)->last_block = TOID_NULL(struct PartitionBlock);
        D_RW(partitionArrayItem)->partition_size = 0;
        D_RW(partitionArrayItem)->numBlocks = 0;
    } TX_ONCOMMIT {
        committed = true;
        block_cv.notify_all();
    } TX_ONABORT {
        fprintf(stderr, "delete Partition of %d_%d_%d failed, type is %d. Error: %s\n", stageId, mapId, partitionId, typeId, pmemobj_errormsg());
        exit(-1);
    } TX_END

    block_cv.wait(block_lck, [&]{return committed;});
    //fprintf(stderr, "request committed %d_%d_%d\n", stageId, mapId, partitionId);

    processed = true;
}
