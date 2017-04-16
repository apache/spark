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
package org.apache.hadoop.fs.ftp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * A {@link FileSystem} backed by an FTP client provided by <a
 * href="http://commons.apache.org/net/">Apache Commons Net</a>.
 * </p>
 */
public class FTPFileSystem extends FileSystem {

  public static final Log LOG = LogFactory
      .getLog(FTPFileSystem.class);

  public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

  public static final int DEFAULT_BLOCK_SIZE = 4 * 1024;

  private URI uri;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException { // get
    super.initialize(uri, conf);
    // get host information from uri (overrides info in conf)
    String host = uri.getHost();
    host = (host == null) ? conf.get("fs.ftp.host", null) : host;
    if (host == null) {
      throw new IOException("Invalid host specified");
    }
    conf.set("fs.ftp.host", host);

    // get port information from uri, (overrides info in conf)
    int port = uri.getPort();
    port = (port == -1) ? FTP.DEFAULT_PORT : port;
    conf.setInt("fs.ftp.host.port", port);

    // get user/password information from URI (overrides info in conf)
    String userAndPassword = uri.getUserInfo();
    if (userAndPassword == null) {
      userAndPassword = (conf.get("fs.ftp.user." + host, null) + ":" + conf
          .get("fs.ftp.password." + host, null));
      if (userAndPassword == null) {
        throw new IOException("Invalid user/passsword specified");
      }
    }
    String[] userPasswdInfo = userAndPassword.split(":");
    conf.set("fs.ftp.user." + host, userPasswdInfo[0]);
    if (userPasswdInfo.length > 1) {
      conf.set("fs.ftp.password." + host, userPasswdInfo[1]);
    } else {
      conf.set("fs.ftp.password." + host, null);
    }
    setConf(conf);
    this.uri = uri;
  }

  /**
   * Connect to the FTP server using configuration parameters *
   * 
   * @return An FTPClient instance
   * @throws IOException
   */
  private FTPClient connect() throws IOException {
    FTPClient client = null;
    Configuration conf = getConf();
    String host = conf.get("fs.ftp.host");
    int port = conf.getInt("fs.ftp.host.port", FTP.DEFAULT_PORT);
    String user = conf.get("fs.ftp.user." + host);
    String password = conf.get("fs.ftp.password." + host);
    client = new FTPClient();
    client.connect(host, port);
    int reply = client.getReplyCode();
    if (!FTPReply.isPositiveCompletion(reply)) {
      throw new IOException("Server - " + host
          + " refused connection on port - " + port);
    } else if (client.login(user, password)) {
      client.setFileTransferMode(FTP.BLOCK_TRANSFER_MODE);
      client.setFileType(FTP.BINARY_FILE_TYPE);
      client.setBufferSize(DEFAULT_BUFFER_SIZE);
    } else {
      throw new IOException("Login failed on server - " + host + ", port - "
          + port);
    }

    return client;
  }

  /**
   * Logout and disconnect the given FTPClient. *
   * 
   * @param client
   * @throws IOException
   */
  private void disconnect(FTPClient client) throws IOException {
    if (client != null) {
      if (!client.isConnected()) {
        throw new FTPException("Client not connected");
      }
      boolean logoutSuccess = client.logout();
      client.disconnect();
      if (!logoutSuccess) {
        LOG.warn("Logout failed while disconnecting, error code - "
            + client.getReplyCode());
      }
    }
  }

  /**
   * Resolve against given working directory. *
   * 
   * @param workDir
   * @param path
   * @return
   */
  private Path makeAbsolute(Path workDir, Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workDir, path);
  }

  @Override
  public FSDataInputStream open(Path file, int bufferSize) throws IOException {
    FTPClient client = connect();
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isDir()) {
      disconnect(client);
      throw new IOException("Path " + file + " is a directory.");
    }
    client.allocate(bufferSize);
    Path parent = absolute.getParent();
    // Change to parent directory on the
    // server. Only then can we read the
    // file
    // on the server by opening up an InputStream. As a side effect the working
    // directory on the server is changed to the parent directory of the file.
    // The FTP client connection is closed when close() is called on the
    // FSDataInputStream.
    client.changeWorkingDirectory(parent.toUri().getPath());
    InputStream is = client.retrieveFileStream(file.getName());
    FSDataInputStream fis = new FSDataInputStream(new FTPInputStream(is,
        client, statistics));
    if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
      // The ftpClient is an inconsistent state. Must close the stream
      // which in turn will logout and disconnect from FTP server
      fis.close();
      throw new IOException("Unable to open file: " + file + ", Aborting");
    }
    return fis;
  }

  /**
   * A stream obtained via this call must be closed before using other APIs of
   * this class or else the invocation will block.
   */
  @Override
  public FSDataOutputStream create(Path file, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    final FTPClient client = connect();
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    if (exists(client, file)) {
      if (overwrite) {
        delete(client, file);
      } else {
        disconnect(client);
        throw new IOException("File already exists: " + file);
      }
    }
    Path parent = absolute.getParent();
    if (parent == null || !mkdirs(client, parent, FsPermission.getDefault())) {
      parent = (parent == null) ? new Path("/") : parent;
      disconnect(client);
      throw new IOException("create(): Mkdirs failed to create: " + parent);
    }
    client.allocate(bufferSize);
    // Change to parent directory on the server. Only then can we write to the
    // file on the server by opening up an OutputStream. As a side effect the
    // working directory on the server is changed to the parent directory of the
    // file. The FTP client connection is closed when close() is called on the
    // FSDataOutputStream.
    client.changeWorkingDirectory(parent.toUri().getPath());
    FSDataOutputStream fos = new FSDataOutputStream(client.storeFileStream(file
        .getName()), statistics) {
      @Override
      public void close() throws IOException {
        super.close();
        if (!client.isConnected()) {
          throw new FTPException("Client not connected");
        }
        boolean cmdCompleted = client.completePendingCommand();
        disconnect(client);
        if (!cmdCompleted) {
          throw new FTPException("Could not complete transfer, Reply Code - "
              + client.getReplyCode());
        }
      }
    };
    if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
      // The ftpClient is an inconsistent state. Must close the stream
      // which in turn will logout and disconnect from FTP server
      fos.close();
      throw new IOException("Unable to create file: " + file + ", Aborting");
    }
    return fos;
  }

  /** This optional operation is not yet supported. */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }
  
  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private boolean exists(FTPClient client, Path file) {
    try {
      return getFileStatus(client, file) != null;
    } catch (FileNotFoundException fnfe) {
      return false;
    } catch (IOException ioe) {
      throw new FTPException("Failed to get file status", ioe);
    }
  }

  /** @deprecated Use delete(Path, boolean) instead */
  @Override
  @Deprecated
  public boolean delete(Path file) throws IOException {
    return delete(file, false);
  }

  @Override
  public boolean delete(Path file, boolean recursive) throws IOException {
    FTPClient client = connect();
    try {
      boolean success = delete(client, file, recursive);
      return success;
    } finally {
      disconnect(client);
    }
  }

  /** @deprecated Use delete(Path, boolean) instead */
  @Deprecated
  private boolean delete(FTPClient client, Path file) throws IOException {
    return delete(client, file, false);
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private boolean delete(FTPClient client, Path file, boolean recursive)
      throws IOException {
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    String pathName = absolute.toUri().getPath();
    FileStatus fileStat = getFileStatus(client, absolute);
    if (!fileStat.isDir()) {
      return client.deleteFile(pathName);
    }
    FileStatus[] dirEntries = listStatus(client, absolute);
    if (dirEntries != null && dirEntries.length > 0 && !(recursive)) {
      throw new IOException("Directory: " + file + " is not empty.");
    }
    if (dirEntries != null) {
      for (int i = 0; i < dirEntries.length; i++) {
        delete(client, new Path(absolute, dirEntries[i].getPath()), recursive);
      }
    }
    return client.removeDirectory(pathName);
  }

  private FsAction getFsAction(int accessGroup, FTPFile ftpFile) {
    FsAction action = FsAction.NONE;
    if (ftpFile.hasPermission(accessGroup, FTPFile.READ_PERMISSION)) {
      action.or(FsAction.READ);
    }
    if (ftpFile.hasPermission(accessGroup, FTPFile.WRITE_PERMISSION)) {
      action.or(FsAction.WRITE);
    }
    if (ftpFile.hasPermission(accessGroup, FTPFile.EXECUTE_PERMISSION)) {
      action.or(FsAction.EXECUTE);
    }
    return action;
  }

  private FsPermission getPermissions(FTPFile ftpFile) {
    FsAction user, group, others;
    user = getFsAction(FTPFile.USER_ACCESS, ftpFile);
    group = getFsAction(FTPFile.GROUP_ACCESS, ftpFile);
    others = getFsAction(FTPFile.WORLD_ACCESS, ftpFile);
    return new FsPermission(user, group, others);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FileStatus[] listStatus(Path file) throws IOException {
    FTPClient client = connect();
    try {
      FileStatus[] stats = listStatus(client, file);
      return stats;
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private FileStatus[] listStatus(FTPClient client, Path file)
      throws IOException {
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    FileStatus fileStat = getFileStatus(client, absolute);
    if (!fileStat.isDir()) {
      return new FileStatus[] { fileStat };
    }
    FTPFile[] ftpFiles = client.listFiles(absolute.toUri().getPath());
    FileStatus[] fileStats = new FileStatus[ftpFiles.length];
    for (int i = 0; i < ftpFiles.length; i++) {
      fileStats[i] = getFileStatus(ftpFiles[i], absolute);
    }
    return fileStats;
  }

  @Override
  public FileStatus getFileStatus(Path file) throws IOException {
    FTPClient client = connect();
    try {
      FileStatus status = getFileStatus(client, file);
      return status;
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private FileStatus getFileStatus(FTPClient client, Path file)
      throws IOException {
    FileStatus fileStat = null;
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    Path parentPath = absolute.getParent();
    if (parentPath == null) { // root dir
      long length = -1; // Length of root dir on server not known
      boolean isDir = true;
      int blockReplication = 1;
      long blockSize = DEFAULT_BLOCK_SIZE; // Block Size not known.
      long modTime = -1; // Modification time of root dir not known.
      Path root = new Path("/");
      return new FileStatus(length, isDir, blockReplication, blockSize,
          modTime, root.makeQualified(this));
    }
    String pathName = parentPath.toUri().getPath();
    FTPFile[] ftpFiles = client.listFiles(pathName);
    if (ftpFiles != null) {
      for (FTPFile ftpFile : ftpFiles) {
        if (ftpFile.getName().equals(file.getName())) { // file found in dir
          fileStat = getFileStatus(ftpFile, parentPath);
          break;
        }
      }
      if (fileStat == null) {
        throw new FileNotFoundException("File " + file + " does not exist.");
      }
    } else {
      throw new FileNotFoundException("File " + file + " does not exist.");
    }
    return fileStat;
  }

  /**
   * Convert the file information in FTPFile to a {@link FileStatus} object. *
   * 
   * @param ftpFile
   * @param parentPath
   * @return FileStatus
   */
  private FileStatus getFileStatus(FTPFile ftpFile, Path parentPath) {
    long length = ftpFile.getSize();
    boolean isDir = ftpFile.isDirectory();
    int blockReplication = 1;
    // Using default block size since there is no way in FTP client to know of
    // block sizes on server. The assumption could be less than ideal.
    long blockSize = DEFAULT_BLOCK_SIZE;
    long modTime = ftpFile.getTimestamp().getTimeInMillis();
    long accessTime = 0;
    FsPermission permission = getPermissions(ftpFile);
    String user = ftpFile.getUser();
    String group = ftpFile.getGroup();
    Path filePath = new Path(parentPath, ftpFile.getName());
    return new FileStatus(length, isDir, blockReplication, blockSize, modTime,
        accessTime, permission, user, group, filePath.makeQualified(this));
  }

  @Override
  public boolean mkdirs(Path file, FsPermission permission) throws IOException {
    FTPClient client = connect();
    try {
      boolean success = mkdirs(client, file, permission);
      return success;
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private boolean mkdirs(FTPClient client, Path file, FsPermission permission)
      throws IOException {
    boolean created = true;
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    String pathName = absolute.getName();
    if (!exists(client, absolute)) {
      Path parent = absolute.getParent();
      created = (parent == null || mkdirs(client, parent, FsPermission
          .getDefault()));
      if (created) {
        String parentDir = parent.toUri().getPath();
        client.changeWorkingDirectory(parentDir);
        created = created & client.makeDirectory(pathName);
      }
    } else if (isFile(client, absolute)) {
      throw new IOException(String.format(
          "Can't make directory for path %s since it is a file.", absolute));
    }
    return created;
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private boolean isFile(FTPClient client, Path file) {
    try {
      return !getFileStatus(client, file).isDir();
    } catch (FileNotFoundException e) {
      return false; // file does not exist
    } catch (IOException ioe) {
      throw new FTPException("File check failed", ioe);
    }
  }

  /*
   * Assuming that parent of both source and destination is the same. Is the
   * assumption correct or it is suppose to work like 'move' ?
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    FTPClient client = connect();
    try {
      boolean success = rename(client, src, dst);
      return success;
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   * 
   * @param client
   * @param src
   * @param dst
   * @return
   * @throws IOException
   */
  private boolean rename(FTPClient client, Path src, Path dst)
      throws IOException {
    Path workDir = new Path(client.printWorkingDirectory());
    Path absoluteSrc = makeAbsolute(workDir, src);
    Path absoluteDst = makeAbsolute(workDir, dst);
    if (!exists(client, absoluteSrc)) {
      throw new IOException("Source path " + src + " does not exist");
    }
    if (exists(client, absoluteDst)) {
      throw new IOException("Destination path " + dst
          + " already exist, cannot rename!");
    }
    String parentSrc = absoluteSrc.getParent().toUri().toString();
    String parentDst = absoluteDst.getParent().toUri().toString();
    String from = src.getName();
    String to = dst.getName();
    if (!parentSrc.equals(parentDst)) {
      throw new IOException("Cannot rename parent(source): " + parentSrc
          + ", parent(destination):  " + parentDst);
    }
    client.changeWorkingDirectory(parentSrc);
    boolean renamed = client.rename(from, to);
    return renamed;
  }

  @Override
  public Path getWorkingDirectory() {
    // Return home directory always since we do not maintain state.
    return getHomeDirectory();
  }

  @Override
  public Path getHomeDirectory() {
    FTPClient client = null;
    try {
      client = connect();
      Path homeDir = new Path(client.printWorkingDirectory());
      return homeDir;
    } catch (IOException ioe) {
      throw new FTPException("Failed to get home directory", ioe);
    } finally {
      try {
        disconnect(client);
      } catch (IOException ioe) {
        throw new FTPException("Failed to disconnect", ioe);
      }
    }
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    // we do not maintain the working directory state
  }
}
