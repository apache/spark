/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "hdfs.h" 

int main(int argc, char **argv) {

    if (argc != 4) {
        fprintf(stderr, "Usage: hdfs_write <filename> <filesize> <buffersize>\n");
        exit(-1);
    }
    
    hdfsFS fs = hdfsConnect("default", 0);
    if (!fs) {
        fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
        exit(-1);
    } 
 
    const char* writeFileName = argv[1];
    tSize fileTotalSize = strtoul(argv[2], NULL, 10);
    tSize bufferSize = strtoul(argv[3], NULL, 10);
   
    hdfsFile writeFile = hdfsOpenFile(fs, writeFileName, O_WRONLY, bufferSize, 0, 0);
    if (!writeFile) {
        fprintf(stderr, "Failed to open %s for writing!\n", writeFileName);
        exit(-2);
    }

    // data to be written to the file
    char* buffer = malloc(sizeof(char) * bufferSize);
    if(buffer == NULL) {
        return -2;
    }
    int i = 0;
    for (i=0; i < bufferSize; ++i) {
        buffer[i] = 'a' + (i%26);
    }
    
    // write to the file
    tSize nrRemaining;
    for (nrRemaining = fileTotalSize; nrRemaining > 0; nrRemaining -= bufferSize ) {
        int curSize = ( bufferSize < nrRemaining ) ? bufferSize : (int)nrRemaining; 
        hdfsWrite(fs, writeFile, (void*)buffer, curSize); 
    }

    free(buffer);
    hdfsCloseFile(fs, writeFile);
    hdfsDisconnect(fs);

    return 0;
}

/**
 * vim: ts=4: sw=4: et:
 */

