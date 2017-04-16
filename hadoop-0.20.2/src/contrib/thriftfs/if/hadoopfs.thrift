
#!/usr/local/bin/thrift -java
#
# Thrift Service exported by Hadoop File System
# Dhruba Borthakur (dhruba@gmail.com)
#

/**
 * The available types in Thrift:
 *
 *  bool        Boolean, one byte
 *  byte        Signed byte
 *  i16         Signed 16-bit integer
 *  i32         Signed 32-bit integer
 *  i64         Signed 64-bit integer
 *  double      64-bit floating point value
 *  string      String
 *  map<t1,t2>  Map from one type to another
 *  list<t1>    Ordered list of one type
 *  set<t1>     Set of unique elements of one type
 *
 */

namespace java org.apache.hadoop.thriftfs.api
namespace php hadoopfs

struct ThriftHandle {
  i64 id
}

struct Pathname {
  string pathname
}

struct FileStatus {
  1: string path,
  2: i64 length,
  3: bool isdir,
  4: i16 block_replication,
  5: i64 blocksize,
  6: i64 modification_time,
  7: string permission,
  8: string owner,
  9: string group
}

struct BlockLocation {
  1: list<string> hosts,         /* hostnames of datanodes */
  2: list<string> names,         /* hostname:portNumber of datanodes */
  3: i64 offset,                 /* offset of the block in the file */
  4: i64 length                  /* length of data */
}

exception MalformedInputException {
  string message
}

exception ThriftIOException {
   string message
}

service ThriftHadoopFileSystem
{

  // set inactivity timeout period. The period is specified in seconds.
  // if there are no RPC calls to the HadoopThrift server for this much
  // time, then the server kills itself.
  void setInactivityTimeoutPeriod(1:i64 periodInSeconds),

  // close session
  void shutdown(1:i32 status),

  // create a file and open it for writing
  ThriftHandle create(1:Pathname path) throws (1:ThriftIOException ouch),

  // create a file and open it for writing
  ThriftHandle createFile(1:Pathname path, 2:i16 mode, 
                          3:bool overwrite, 4:i32 bufferSize, 
                          5:i16 block_replication, 6:i64 blocksize) 
                          throws (1:ThriftIOException ouch),

  // returns a handle to an existing file  for reading
  ThriftHandle open(1:Pathname path) throws (1:ThriftIOException ouch),

  // returns a handle to an existing file for appending to it.
  ThriftHandle append(1:Pathname path) throws (1:ThriftIOException ouch),

  // write a string to the open handle for the file
  bool write(1:ThriftHandle handle, string data) throws (1:ThriftIOException ouch),

  // read some bytes from the open handle for the file
  string read(1:ThriftHandle handle, i64 offset, i32 size) throws (1:ThriftIOException ouch),

  // close file
  bool close(1:ThriftHandle out) throws (1:ThriftIOException ouch),

  // delete file(s) or directory(s)
  bool rm(1:Pathname path, 2:bool recursive) throws (1:ThriftIOException ouch),

  // rename file(s) or directory(s)
  bool rename(1:Pathname path, 2:Pathname dest) throws (1:ThriftIOException ouch),

  // create directory
  bool mkdirs(1:Pathname path) throws (1:ThriftIOException ouch),

  // Does this pathname exist?
  bool exists(1:Pathname path) throws (1:ThriftIOException ouch),

  // Returns status about the path
  FileStatus stat(1:Pathname path) throws (1:ThriftIOException ouch),

  // If the path is a directory, then returns the list of pathnames in that directory
  list<FileStatus> listStatus(1:Pathname path) throws (1:ThriftIOException ouch),

  // Set permission for this file
  void chmod(1:Pathname path, 2:i16 mode) throws (1:ThriftIOException ouch),

  // set the owner and group of the file.
  void chown(1:Pathname path, 2:string owner, 3:string group) throws (1:ThriftIOException ouch),

  // set the replication factor for all blocks of the specified file
  void setReplication(1:Pathname path, 2:i16 replication) throws (1:ThriftIOException ouch),

  // get the locations of the blocks of this file
  list<BlockLocation> getFileBlockLocations(1:Pathname path, 2:i64 start, 3:i64 length) throws (1:ThriftIOException ouch),
}
