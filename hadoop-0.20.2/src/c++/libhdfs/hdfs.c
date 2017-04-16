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
#include "hdfsJniHelper.h"


/* Some frequently used Java paths */
#define HADOOP_CONF     "org/apache/hadoop/conf/Configuration"
#define HADOOP_PATH     "org/apache/hadoop/fs/Path"
#define HADOOP_LOCALFS  "org/apache/hadoop/fs/LocalFileSystem"
#define HADOOP_FS       "org/apache/hadoop/fs/FileSystem"
#define HADOOP_FSSTATUS "org/apache/hadoop/fs/FsStatus"
#define HADOOP_BLK_LOC  "org/apache/hadoop/fs/BlockLocation"
#define HADOOP_DFS      "org/apache/hadoop/hdfs/DistributedFileSystem"
#define HADOOP_ISTRM    "org/apache/hadoop/fs/FSDataInputStream"
#define HADOOP_OSTRM    "org/apache/hadoop/fs/FSDataOutputStream"
#define HADOOP_STAT     "org/apache/hadoop/fs/FileStatus"
#define HADOOP_FSPERM   "org/apache/hadoop/fs/permission/FsPermission"
#define JAVA_NET_ISA    "java/net/InetSocketAddress"
#define JAVA_NET_URI    "java/net/URI"
#define JAVA_STRING     "java/lang/String"

#define JAVA_VOID       "V"

/* Macros for constructing method signatures */
#define JPARAM(X)           "L" X ";"
#define JARRPARAM(X)        "[L" X ";"
#define JMETHOD1(X, R)      "(" X ")" R
#define JMETHOD2(X, Y, R)   "(" X Y ")" R
#define JMETHOD3(X, Y, Z, R)   "(" X Y Z")" R


/**
 * hdfsJniEnv: A wrapper struct to be used as 'value'
 * while saving thread -> JNIEnv* mappings
 */
typedef struct
{
    JNIEnv* env;
} hdfsJniEnv;



/**
 * Helper function to destroy a local reference of java.lang.Object
 * @param env: The JNIEnv pointer. 
 * @param jFile: The local reference of java.lang.Object object
 * @return None.
 */
static void destroyLocalReference(JNIEnv *env, jobject jObject)
{
  if (jObject)
    (*env)->DeleteLocalRef(env, jObject);
}


/**
 * Helper function to create a org.apache.hadoop.fs.Path object.
 * @param env: The JNIEnv pointer. 
 * @param path: The file-path for which to construct org.apache.hadoop.fs.Path
 * object.
 * @return Returns a jobject on success and NULL on error.
 */
static jobject constructNewObjectOfPath(JNIEnv *env, const char *path)
{
    //Construct a java.lang.String object
    jstring jPathString = (*env)->NewStringUTF(env, path); 

    //Construct the org.apache.hadoop.fs.Path object
    jobject jPath =
        constructNewObjectOfClass(env, NULL, "org/apache/hadoop/fs/Path",
                                  "(Ljava/lang/String;)V", jPathString);
    if (jPath == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.fs.Path for %s\n", path);
        errno = EINTERNAL;
        return NULL;
    }

    // Destroy the local reference to the java.lang.String object
    destroyLocalReference(env, jPathString);

    return jPath;
}


/**
 * Helper function to translate an exception into a meaningful errno value.
 * @param exc: The exception.
 * @param env: The JNIEnv Pointer.
 * @param method: The name of the method that threw the exception. This
 * may be format string to be used in conjuction with additional arguments.
 * @return Returns a meaningful errno value if possible, or EINTERNAL if not.
 */
static int errnoFromException(jthrowable exc, JNIEnv *env,
                              const char *method, ...)
{
    va_list ap;
    int errnum = 0;
    char *excClass = NULL;

    if (exc == NULL)
        goto default_error;

    if ((excClass = classNameOfObject((jobject) exc, env)) == NULL) {
      errnum = EINTERNAL;
      goto done;
    }

    if (!strcmp(excClass, "org.apache.hadoop.security."
                "AccessControlException")) {
        errnum = EACCES;
        goto done;
    }

    if (!strcmp(excClass, "org.apache.hadoop.hdfs.protocol."
                "QuotaExceededException")) {
        errnum = EDQUOT;
        goto done;
    }

    if (!strcmp(excClass, "java.io.FileNotFoundException")) {
        errnum = ENOENT;
        goto done;
    }

    //TODO: interpret more exceptions; maybe examine exc.getMessage()

default_error:

    //Can't tell what went wrong, so just punt
    (*env)->ExceptionDescribe(env);
    fprintf(stderr, "Call to ");
    va_start(ap, method);
    vfprintf(stderr, method, ap);
    va_end(ap);
    fprintf(stderr, " failed!\n");
    errnum = EINTERNAL;

done:

    (*env)->ExceptionClear(env);

    if (excClass != NULL)
        free(excClass);

    return errnum;
}




hdfsFS hdfsConnect(const char* host, tPort port) {
  // connect with NULL as user name
  return hdfsConnectAsUser(host, port, NULL);
}

/** Always return a new FileSystem handle */
hdfsFS hdfsConnectNewInstance(const char* host, tPort port) {
  // connect with NULL as user name/groups
  return hdfsConnectAsUserNewInstance(host, port, NULL);
}

hdfsFS hdfsConnectAsUser(const char* host, tPort port, const char *user)
{
    // JAVA EQUIVALENT:
    //  FileSystem fs = FileSystem.get(new Configuration());
    //  return fs;

    JNIEnv *env = 0;
    jobject jConfiguration = NULL;
    jobject jFS = NULL;
    jobject jURI = NULL;
    jstring jURIString = NULL;
    jvalue  jVal;
    jthrowable jExc = NULL;
    char    *cURI = 0;
    jobject gFsRef = NULL;
    jstring jUserString = NULL;


    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    //Create the org.apache.hadoop.conf.Configuration object
    jConfiguration =
        constructNewObjectOfClass(env, NULL, HADOOP_CONF, "()V");

    if (jConfiguration == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.conf.Configuration\n");
        errno = EINTERNAL;
        return NULL;
    }
 
    if (user != NULL) {
      jUserString = (*env)->NewStringUTF(env, user);
    }
    //Check what type of FileSystem the caller wants...
    if (host == NULL) {
        // fs = FileSytem::getLocal(conf);
        if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, HADOOP_FS, "getLocal",
                         JMETHOD1(JPARAM(HADOOP_CONF),
                                  JPARAM(HADOOP_LOCALFS)),
                         jConfiguration) != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                       "FileSystem::getLocal");
            goto done;
        }
        jFS = jVal.l;
    }
    //FileSystem.get(conf) -> FileSystem.get(FileSystem.getDefaultUri(conf), 
    //                                       conf, user)
    else if (!strcmp(host, "default") && port == 0) {
      if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, HADOOP_FS,
                      "getDefaultUri", 
                      "(Lorg/apache/hadoop/conf/Configuration;)Ljava/net/URI;",
                      jConfiguration) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs.", 
                                   "FileSystem::getDefaultUri");
        goto done;
      }
      jURI = jVal.l;
      if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, HADOOP_FS, "get",
                       JMETHOD3(JPARAM(JAVA_NET_URI),
                                JPARAM(HADOOP_CONF), JPARAM(JAVA_STRING), 
                                JPARAM(HADOOP_FS)),
                       jURI, jConfiguration, jUserString) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "Filesystem::get(URI, Configuration)");
        goto done;
      }

      jFS = jVal.l;
    }
    else {
      // fs = FileSystem::get(URI, conf, ugi);
      cURI = malloc(strlen(host)+16);
      sprintf(cURI, "hdfs://%s:%d", host, (int)(port));
      if (cURI == NULL) {
        fprintf (stderr, "Couldn't allocate an object of size %d",
                 (int)(strlen(host) + 16));
        errno = EINTERNAL;			
        goto done;	
      }

      jURIString = (*env)->NewStringUTF(env, cURI);
      if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, JAVA_NET_URI,
                       "create", "(Ljava/lang/String;)Ljava/net/URI;",
                       jURIString) != 0) {
        errno = errnoFromException(jExc, env, "java.net.URI::create");
        goto done;
      }
      jURI = jVal.l;

      if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, HADOOP_FS, "get",
                       JMETHOD3(JPARAM(JAVA_NET_URI),
                                JPARAM(HADOOP_CONF), JPARAM(JAVA_STRING),
                                JPARAM(HADOOP_FS)),
                       jURI, jConfiguration, jUserString) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "Filesystem::get(URI, Configuration)");
        goto done;
      }

      jFS = jVal.l;
    }

  done:
    
    // Release unnecessary local references
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jURIString);
    destroyLocalReference(env, jURI);
    destroyLocalReference(env, jUserString);

    if (cURI) free(cURI);

    /* Create a global reference for this fs */
    if (jFS) {
        gFsRef = (*env)->NewGlobalRef(env, jFS);
        destroyLocalReference(env, jFS);
    }

    return gFsRef;
}


/** Always return a new FileSystem handle */
hdfsFS hdfsConnectAsUserNewInstance(const char* host, tPort port, const char *user)
{
    // JAVA EQUIVALENT:
    //  FileSystem fs = FileSystem.get(new Configuration());
    //  return fs;

    JNIEnv *env = 0;
    jobject jConfiguration = NULL;
    jobject jFS = NULL;
    jobject jURI = NULL;
    jstring jURIString = NULL;
    jvalue  jVal;
    jthrowable jExc = NULL;
    char    *cURI = 0;
    jobject gFsRef = NULL;
    jstring jUserString = NULL;

    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    //Create the org.apache.hadoop.conf.Configuration object
    jConfiguration =
        constructNewObjectOfClass(env, NULL, HADOOP_CONF, "()V");

    if (jConfiguration == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.conf.Configuration\n");
        errno = EINTERNAL;
        return NULL;
    }
 
    if (user != NULL) {
      jUserString = (*env)->NewStringUTF(env, user);
    }      
    //Check what type of FileSystem the caller wants...
    if (host == NULL) {
        // fs = FileSytem::newInstanceLocal(conf);
        if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, HADOOP_FS, "newInstanceLocal",
                         JMETHOD1(JPARAM(HADOOP_CONF),
                                  JPARAM(HADOOP_LOCALFS)),
                         jConfiguration) != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                       "FileSystem::newInstanceLocal");
            goto done;
        }
        jFS = jVal.l;
    }
    else if (!strcmp(host, "default") && port == 0) {
      //fs = FileSystem::get(conf); 
      if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, HADOOP_FS,
                      "getDefaultUri",
                      "(Lorg/apache/hadoop/conf/Configuration;)Ljava/net/URI;",
                      jConfiguration) != 0) {
          errno = errnoFromException(jExc, env, "org.apache.hadoop.fs.",
                                   "FileSystem::getDefaultUri");
          goto done;
      }
      jURI = jVal.l;
      if (invokeMethod(env, &jVal, &jExc, STATIC, NULL,
                         HADOOP_FS, "newInstance",
                         JMETHOD3(JPARAM(JAVA_NET_URI),
                                  JPARAM(HADOOP_CONF),
                                  JPARAM(JAVA_STRING),
                                  JPARAM(HADOOP_FS)),
                         jURI, jConfiguration, jUserString) != 0) {
          errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                       "FileSystem::newInstance");
          goto done;
      }
      jFS = jVal.l;
    }
    else {
        // fs = FileSystem::newInstance(URI, conf);
        cURI = malloc(strlen(host)+16);
        sprintf(cURI, "hdfs://%s:%d", host, (int)(port));

        jURIString = (*env)->NewStringUTF(env, cURI);
        if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, JAVA_NET_URI,
                         "create", "(Ljava/lang/String;)Ljava/net/URI;",
                         jURIString) != 0) {
            errno = errnoFromException(jExc, env, "java.net.URI::create");
            goto done;
        }
        jURI = jVal.l;

        if (invokeMethod(env, &jVal, &jExc, STATIC, NULL, HADOOP_FS, "newInstance",
                         JMETHOD3(JPARAM(JAVA_NET_URI),
                                  JPARAM(HADOOP_CONF), JPARAM(JAVA_STRING),
                                  JPARAM(HADOOP_FS)),
                         jURI, jConfiguration, jUserString) != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                       "Filesystem::newInstance(URI, Configuration)");
            goto done;
        }

        jFS = jVal.l;
    }

  done:
    
    // Release unnecessary local references
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jURIString);
    destroyLocalReference(env, jURI);
    destroyLocalReference(env, jUserString);

    if (cURI) free(cURI);

    /* Create a global reference for this fs */
    if (jFS) {
        gFsRef = (*env)->NewGlobalRef(env, jFS);
        destroyLocalReference(env, jFS);
    }

    return gFsRef;
}

int hdfsDisconnect(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.close()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
      errno = EINTERNAL;
      return -2;
    }

    //Parameters
    jobject jFS = (jobject)fs;

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (fs == NULL) {
        errno = EBADF;
        return -1;
    }

    if (invokeMethod(env, NULL, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "close", "()V") != 0) {
        errno = errnoFromException(jExc, env, "Filesystem::close");
        return -1;
    }

    //Release unnecessary references
    (*env)->DeleteGlobalRef(env, fs);

    return 0;
}



hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags, 
                      int bufferSize, short replication, tSize blockSize)
{
    /*
      JAVA EQUIVALENT:
       File f = new File(path);
       FSData{Input|Output}Stream f{is|os} = fs.create(f);
       return f{is|os};
    */
    /* Get the JNIEnv* corresponding to current thread */
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;

    if (flags & O_RDWR) {
      fprintf(stderr, "ERROR: cannot open an hdfs file in O_RDWR mode\n");
      errno = ENOTSUP;
      return NULL;
    }

    if ((flags & O_CREAT) && (flags & O_EXCL)) {
      fprintf(stderr, "WARN: hdfs does not truly support O_CREATE && O_EXCL\n");
    }

    /* The hadoop java api/signature */
    const char* method = ((flags & O_WRONLY) == 0) ? "open" : (flags & O_APPEND) ? "append" : "create";
    const char* signature = ((flags & O_WRONLY) == 0) ?
        JMETHOD2(JPARAM(HADOOP_PATH), "I", JPARAM(HADOOP_ISTRM)) :
      (flags & O_APPEND) ?
      JMETHOD1(JPARAM(HADOOP_PATH), JPARAM(HADOOP_OSTRM)) :
      JMETHOD2(JPARAM(HADOOP_PATH), "ZISJ", JPARAM(HADOOP_OSTRM));

    /* Return value */
    hdfsFile file = NULL;

    /* Create an object of org.apache.hadoop.fs.Path */
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL; 
    }

    /* Get the Configuration object from the FileSystem object */
    jvalue  jVal;
    jobject jConfiguration = NULL;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "getConf", JMETHOD1("", JPARAM(HADOOP_CONF))) != 0) {
        errno = errnoFromException(jExc, env, "get configuration object "
                                   "from filesystem");
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jConfiguration = jVal.l;

    jint jBufferSize = bufferSize;
    jshort jReplication = replication;
    jlong jBlockSize = blockSize;
    jstring jStrBufferSize = (*env)->NewStringUTF(env, "io.file.buffer.size"); 
    jstring jStrReplication = (*env)->NewStringUTF(env, "dfs.replication");
    jstring jStrBlockSize = (*env)->NewStringUTF(env, "dfs.block.size");


    //bufferSize
    if (!bufferSize) {
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jConfiguration, 
                         HADOOP_CONF, "getInt", "(Ljava/lang/String;I)I",
                         jStrBufferSize, 4096) != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                                       "Configuration::getInt");
            goto done;
        }
        jBufferSize = jVal.i;
    }

    if ((flags & O_WRONLY) && (flags & O_APPEND) == 0) {
        //replication

        if (!replication) {
            if (invokeMethod(env, &jVal, &jExc, INSTANCE, jConfiguration, 
                             HADOOP_CONF, "getInt", "(Ljava/lang/String;I)I",
                             jStrReplication, 1) != 0) {
                errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                                           "Configuration::getInt");
                goto done;
            }
            jReplication = jVal.i;
        }
        
        //blockSize
        if (!blockSize) {
            if (invokeMethod(env, &jVal, &jExc, INSTANCE, jConfiguration, 
                             HADOOP_CONF, "getLong", "(Ljava/lang/String;J)J",
                             jStrBlockSize, (jlong)67108864)) {
                errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                                           "FileSystem::%s(%s)", method,
                                           signature);
                goto done;
            }
            jBlockSize = jVal.j;
        }
    }
 
    /* Create and return either the FSDataInputStream or
       FSDataOutputStream references jobject jStream */

    // READ?
    if ((flags & O_WRONLY) == 0) {
      if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                       method, signature, jPath, jBufferSize)) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                                   "FileSystem::%s(%s)", method,
                                   signature);
        goto done;
      }
    }  else if ((flags & O_WRONLY) && (flags & O_APPEND)) {
      // WRITE/APPEND?
       if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                       method, signature, jPath)) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                                   "FileSystem::%s(%s)", method,
                                   signature);
        goto done;
      }
    } else {
        // WRITE/CREATE
        jboolean jOverWrite = 1;
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                         method, signature, jPath, jOverWrite,
                         jBufferSize, jReplication, jBlockSize)) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.conf."
                                       "FileSystem::%s(%s)", method,
                                       signature);
            goto done;
        }
    }
  
    file = malloc(sizeof(struct hdfsFile_internal));
    if (!file) {
        errno = ENOMEM;
    } else {
        file->file = (*env)->NewGlobalRef(env, jVal.l);
        file->type = (((flags & O_WRONLY) == 0) ? INPUT : OUTPUT);

        destroyLocalReference(env, jVal.l);
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jStrBufferSize);
    destroyLocalReference(env, jStrReplication);
    destroyLocalReference(env, jStrBlockSize);
    destroyLocalReference(env, jConfiguration); 
    destroyLocalReference(env, jPath); 

    return file;
}



int hdfsCloseFile(hdfsFS fs, hdfsFile file)
{
    // JAVA EQUIVALENT:
    //  file.close 

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
      errno = EINTERNAL;
      return -2;
    }

    //Parameters
    jobject jStream = (jobject)(file ? file->file : NULL);

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (!file || file->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //The interface whose 'close' method to be called
    const char* interface = (file->type == INPUT) ? 
        HADOOP_ISTRM : HADOOP_OSTRM;
  
    if (invokeMethod(env, NULL, &jExc, INSTANCE, jStream, interface,
                     "close", "()V") != 0) {
        errno = errnoFromException(jExc, env, "%s::close", interface);
        return -1;
    }

    //De-allocate memory
    free(file);
    (*env)->DeleteGlobalRef(env, jStream);

    return 0;
}



int hdfsExists(hdfsFS fs, const char *path)
{
    JNIEnv *env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -2;
    }

    jobject jPath = constructNewObjectOfPath(env, path);
    jvalue  jVal;
    jthrowable jExc = NULL;
    jobject jFS = (jobject)fs;

    if (jPath == NULL) {
        return -1;
    }

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "exists", JMETHOD1(JPARAM(HADOOP_PATH), "Z"),
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::exists");
        destroyLocalReference(env, jPath);
        return -1;
    }

    destroyLocalReference(env, jPath);
    return jVal.z ? 0 : -1;
}



tSize hdfsRead(hdfsFS fs, hdfsFile f, void* buffer, tSize length)
{
    // JAVA EQUIVALENT:
    //  byte [] bR = new byte[length];
    //  fis.read(bR);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jInputStream = (jobject)(f ? f->file : NULL);

    jbyteArray jbRarray;
    jint noReadBytes = 0;
    jvalue jVal;
    jthrowable jExc = NULL;

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //Error checking... make sure that this file is 'readable'
    if (f->type != INPUT) {
        fprintf(stderr, "Cannot read from a non-InputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    //Read the requisite bytes
    jbRarray = (*env)->NewByteArray(env, length);
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jInputStream, HADOOP_ISTRM,
                     "read", "([B)I", jbRarray) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FSDataInputStream::read");
        noReadBytes = -1;
    }
    else {
        noReadBytes = jVal.i;
        if (noReadBytes > 0) {
            (*env)->GetByteArrayRegion(env, jbRarray, 0, noReadBytes, buffer);
        }  else {
            //This is a valid case: there aren't any bytes left to read!
          if (noReadBytes == 0 || noReadBytes < -1) {
            fprintf(stderr, "WARN: FSDataInputStream.read returned invalid return code - libhdfs returning EOF, i.e., 0: %d\n", noReadBytes);
          }
            noReadBytes = 0;
        }
        errno = 0;
    }

    destroyLocalReference(env, jbRarray);

    return noReadBytes;
}


  
tSize hdfsPread(hdfsFS fs, hdfsFile f, tOffset position,
                void* buffer, tSize length)
{
    // JAVA EQUIVALENT:
    //  byte [] bR = new byte[length];
    //  fis.read(pos, bR, 0, length);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jInputStream = (jobject)(f ? f->file : NULL);

    jbyteArray jbRarray;
    jint noReadBytes = 0;
    jvalue jVal;
    jthrowable jExc = NULL;

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //Error checking... make sure that this file is 'readable'
    if (f->type != INPUT) {
        fprintf(stderr, "Cannot read from a non-InputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    //Read the requisite bytes
    jbRarray = (*env)->NewByteArray(env, length);
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jInputStream, HADOOP_ISTRM,
                     "read", "(J[BII)I", position, jbRarray, 0, length) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FSDataInputStream::read");
        noReadBytes = -1;
    }
    else {
        noReadBytes = jVal.i;
        if (noReadBytes > 0) {
            (*env)->GetByteArrayRegion(env, jbRarray, 0, noReadBytes, buffer);
        }  else {
            //This is a valid case: there aren't any bytes left to read!
          if (noReadBytes == 0 || noReadBytes < -1) {
            fprintf(stderr, "WARN: FSDataInputStream.read returned invalid return code - libhdfs returning EOF, i.e., 0: %d\n", noReadBytes);
          }
            noReadBytes = 0;
        }
        errno = 0;
    }
    destroyLocalReference(env, jbRarray);

    return noReadBytes;
}



tSize hdfsWrite(hdfsFS fs, hdfsFile f, const void* buffer, tSize length)
{
    // JAVA EQUIVALENT
    // byte b[] = str.getBytes();
    // fso.write(b);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jOutputStream = (jobject)(f ? f->file : 0);
    jbyteArray jbWarray;

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }
    
    if (length < 0) {
    	errno = EINVAL;
    	return -1;
    }

    //Error checking... make sure that this file is 'writable'
    if (f->type != OUTPUT) {
        fprintf(stderr, "Cannot write into a non-OutputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    // 'length' equals 'zero' is a valid use-case according to Posix!
    if (length != 0) {
        //Write the requisite bytes into the file
        jbWarray = (*env)->NewByteArray(env, length);
        (*env)->SetByteArrayRegion(env, jbWarray, 0, length, buffer);
        if (invokeMethod(env, NULL, &jExc, INSTANCE, jOutputStream,
                         HADOOP_OSTRM, "write",
                         "([B)V", jbWarray) != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                       "FSDataOutputStream::write");
            length = -1;
        }
        destroyLocalReference(env, jbWarray);
    }

    //Return no. of bytes succesfully written (libc way)
    //i.e. 'length' itself! ;-)
    return length;
}



int hdfsSeek(hdfsFS fs, hdfsFile f, tOffset desiredPos) 
{
    // JAVA EQUIVALENT
    //  fis.seek(pos);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jInputStream = (jobject)(f ? f->file : 0);

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (!f || f->type != INPUT) {
        errno = EBADF;
        return -1;
    }

    if (invokeMethod(env, NULL, &jExc, INSTANCE, jInputStream, HADOOP_ISTRM,
                     "seek", "(J)V", desiredPos) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FSDataInputStream::seek");
        return -1;
    }

    return 0;
}



tOffset hdfsTell(hdfsFS fs, hdfsFile f)
{
    // JAVA EQUIVALENT
    //  pos = f.getPos();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jStream = (jobject)(f ? f->file : 0);

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    const char* interface = (f->type == INPUT) ?
        HADOOP_ISTRM : HADOOP_OSTRM;

    jlong currentPos  = -1;
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStream,
                     interface, "getPos", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FSDataInputStream::getPos");
        return -1;
    }
    currentPos = jVal.j;

    return (tOffset)currentPos;
}



int hdfsFlush(hdfsFS fs, hdfsFile f) 
{
    // JAVA EQUIVALENT
    //  fos.flush();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jOutputStream = (jobject)(f ? f->file : 0);

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (!f || f->type != OUTPUT) {
        errno = EBADF;
        return -1;
    }

    if (invokeMethod(env, NULL, &jExc, INSTANCE, jOutputStream, 
                     HADOOP_OSTRM, "flush", "()V") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FSDataInputStream::flush");
        return -1;
    }

    return 0;
}



int hdfsHFlush(hdfsFS fs, hdfsFile f)
{
    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jOutputStream = (jobject)(f ? f->file : 0);

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (!f || f->type != OUTPUT) {
        errno = EBADF;
        return -1;
    }

    // NB: Use sync, which has been deprecated in favor of hflush,
    // however hflush is not available in 0.20.
    if (invokeMethod(env, NULL, &jExc, INSTANCE, jOutputStream,
                     HADOOP_OSTRM, "sync", "()V") != 0) {
        errno = errnoFromException(jExc, env, HADOOP_OSTRM "::sync");
        return -1;
    }

    return 0;
}



int hdfsAvailable(hdfsFS fs, hdfsFile f)
{
    // JAVA EQUIVALENT
    //  fis.available();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jInputStream = (jobject)(f ? f->file : 0);

    //Caught exception
    jthrowable jExc = NULL;

    //Sanity check
    if (!f || f->type != INPUT) {
        errno = EBADF;
        return -1;
    }

    jint available = -1;
    jvalue jVal;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jInputStream, 
                     HADOOP_ISTRM, "available", "()I") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FSDataInputStream::available");
        return -1;
    }
    available = jVal.i;

    return available;
}



int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    //JAVA EQUIVALENT
    //  FileUtil::copy(srcFS, srcPath, dstFS, dstPath,
    //                 deleteSource = false, conf)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jobject jSrcFS = (jobject)srcFS;
    jobject jDstFS = (jobject)dstFS;
    jobject jSrcPath = NULL;
    jobject jDstPath = NULL;

    jSrcPath = constructNewObjectOfPath(env, src);
    if (jSrcPath == NULL) {
        return -1;
    }

    jDstPath = constructNewObjectOfPath(env, dst);
    if (jDstPath == NULL) {
        destroyLocalReference(env, jSrcPath);
        return -1;
    }

    int retval = 0;

    //Create the org.apache.hadoop.conf.Configuration object
    jobject jConfiguration =
        constructNewObjectOfClass(env, NULL, HADOOP_CONF, "()V");
    if (jConfiguration == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.conf.Configuration\n");
        errno = EINTERNAL;
        destroyLocalReference(env, jSrcPath);
        destroyLocalReference(env, jDstPath);
        return -1;
    }

    //FileUtil::copy
    jboolean deleteSource = 0; //Only copy
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, STATIC, 
                     NULL, "org/apache/hadoop/fs/FileUtil", "copy",
                     "(Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;ZLorg/apache/hadoop/conf/Configuration;)Z",
                     jSrcFS, jSrcPath, jDstFS, jDstPath, deleteSource, 
                     jConfiguration) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileUtil::copy");
        retval = -1;
        goto done;
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jSrcPath);
    destroyLocalReference(env, jDstPath);
  
    return retval;
}



int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    //JAVA EQUIVALENT
    //  FileUtil::copy(srcFS, srcPath, dstFS, dstPath,
    //                 deleteSource = true, conf)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }


    //Parameters
    jobject jSrcFS = (jobject)srcFS;
    jobject jDstFS = (jobject)dstFS;

    jobject jSrcPath = NULL;
    jobject jDstPath = NULL;

    jSrcPath = constructNewObjectOfPath(env, src);
    if (jSrcPath == NULL) {
        return -1;
    }

    jDstPath = constructNewObjectOfPath(env, dst);
    if (jDstPath == NULL) {
        destroyLocalReference(env, jSrcPath);
        return -1;
    }

    int retval = 0;

    //Create the org.apache.hadoop.conf.Configuration object
    jobject jConfiguration =
        constructNewObjectOfClass(env, NULL, HADOOP_CONF, "()V");
    if (jConfiguration == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.conf.Configuration\n");
        errno = EINTERNAL;
        destroyLocalReference(env, jSrcPath);
        destroyLocalReference(env, jDstPath);
        return -1;
    }

    //FileUtil::copy
    jboolean deleteSource = 1; //Delete src after copy
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, STATIC, NULL,
                     "org/apache/hadoop/fs/FileUtil", "copy",
                "(Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;ZLorg/apache/hadoop/conf/Configuration;)Z",
                     jSrcFS, jSrcPath, jDstFS, jDstPath, deleteSource, 
                     jConfiguration) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileUtil::copy(move)");
        retval = -1;
        goto done;
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jSrcPath);
    destroyLocalReference(env, jDstPath);
  
    return retval;
}



int hdfsDelete(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  File f = new File(path);
    //  bool retval = fs.delete(f);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //Create an object of java.io.File
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Delete the file
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "delete", "(Lorg/apache/hadoop/fs/Path;)Z",
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::delete");
        destroyLocalReference(env, jPath);
        return -1;
    }

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return (jVal.z) ? 0 : -1;
}



int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath)
{
    // JAVA EQUIVALENT:
    //  Path old = new Path(oldPath);
    //  Path new = new Path(newPath);
    //  fs.rename(old, new);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //Create objects of org.apache.hadoop.fs.Path
    jobject jOldPath = NULL;
    jobject jNewPath = NULL;

    jOldPath = constructNewObjectOfPath(env, oldPath);
    if (jOldPath == NULL) {
        return -1;
    }

    jNewPath = constructNewObjectOfPath(env, newPath);
    if (jNewPath == NULL) {
        destroyLocalReference(env, jOldPath);
        return -1;
    }

    //Rename the file
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS, "rename",
                     JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_PATH), "Z"),
                     jOldPath, jNewPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::rename");
        destroyLocalReference(env, jOldPath);
        destroyLocalReference(env, jNewPath);
        return -1;
    }

    //Delete unnecessary local references
    destroyLocalReference(env, jOldPath);
    destroyLocalReference(env, jNewPath);

    return (jVal.z) ? 0 : -1;
}



char* hdfsGetWorkingDirectory(hdfsFS fs, char* buffer, size_t bufferSize)
{
    // JAVA EQUIVALENT:
    //  Path p = fs.getWorkingDirectory(); 
    //  return p.toString()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;
    jobject jPath = NULL;
    jvalue jVal;
    jthrowable jExc = NULL;

    //FileSystem::getWorkingDirectory()
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS,
                     HADOOP_FS, "getWorkingDirectory",
                     "()Lorg/apache/hadoop/fs/Path;") != 0 ||
        jVal.l == NULL) {
        errno = errnoFromException(jExc, env, "FileSystem::"
                                   "getWorkingDirectory");
        return NULL;
    }
    jPath = jVal.l;

    //Path::toString()
    jstring jPathString;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jPath, 
                     "org/apache/hadoop/fs/Path", "toString",
                     "()Ljava/lang/String;") != 0) { 
        errno = errnoFromException(jExc, env, "Path::toString");
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jPathString = jVal.l;

    const char *jPathChars = (const char*)
        ((*env)->GetStringUTFChars(env, jPathString, NULL));

    //Copy to user-provided buffer
    strncpy(buffer, jPathChars, bufferSize);

    //Delete unnecessary local references
    (*env)->ReleaseStringUTFChars(env, jPathString, jPathChars);

    destroyLocalReference(env, jPathString);
    destroyLocalReference(env, jPath);

    return buffer;
}



int hdfsSetWorkingDirectory(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  fs.setWorkingDirectory(Path(path)); 

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;
    int retval = 0;
    jthrowable jExc = NULL;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //FileSystem::setWorkingDirectory()
    if (invokeMethod(env, NULL, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "setWorkingDirectory", 
                     "(Lorg/apache/hadoop/fs/Path;)V", jPath) != 0) {
        errno = errnoFromException(jExc, env, "FileSystem::"
                                   "setWorkingDirectory");
        retval = -1;
    }

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return retval;
}



int hdfsCreateDirectory(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  fs.mkdirs(new Path(path));

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Create the directory
    jvalue jVal;
    jVal.z = 0;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "mkdirs", "(Lorg/apache/hadoop/fs/Path;)Z",
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::mkdirs");
        goto done;
    }

 done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return (jVal.z) ? 0 : -1;
}


int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication)
{
    // JAVA EQUIVALENT:
    //  fs.setReplication(new Path(path), replication);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Create the directory
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "setReplication", "(Lorg/apache/hadoop/fs/Path;S)Z",
                     jPath, replication) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::setReplication");
        goto done;
    }

 done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return (jVal.z) ? 0 : -1;
}

int hdfsChown(hdfsFS fs, const char* path, const char *owner, const char *group)
{
    // JAVA EQUIVALENT:
    //  fs.setOwner(path, owner, group)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    if (owner == NULL && group == NULL) {
      fprintf(stderr, "Both owner and group cannot be null in chown");
      errno = EINVAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    jstring jOwnerString = (*env)->NewStringUTF(env, owner); 
    jstring jGroupString = (*env)->NewStringUTF(env, group); 

    //Create the directory
    int ret = 0;
    jthrowable jExc = NULL;
    if (invokeMethod(env, NULL, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "setOwner", JMETHOD3(JPARAM(HADOOP_PATH), JPARAM(JAVA_STRING), JPARAM(JAVA_STRING), JAVA_VOID),
                     jPath, jOwnerString, jGroupString) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::setOwner");
        ret = -1;
        goto done;
    }

 done:
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jOwnerString);
    destroyLocalReference(env, jGroupString);

    return ret;
}

int hdfsChmod(hdfsFS fs, const char* path, short mode)
{
    // JAVA EQUIVALENT:
    //  fs.setPermission(path, FsPermission)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    // construct jPerm = FsPermission.createImmutable(short mode);

    jshort jmode = mode;

    jobject jPermObj =
      constructNewObjectOfClass(env, NULL, HADOOP_FSPERM,"(S)V",jmode);
    if (jPermObj == NULL) {
      return -2;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
      destroyLocalReference(env, jPermObj);
      return -3;
    }

    //Create the directory
    int ret = 0;
    jthrowable jExc = NULL;
    if (invokeMethod(env, NULL, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "setPermission", JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_FSPERM), JAVA_VOID),
                     jPath, jPermObj) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::setPermission");
        ret = -1;
        goto done;
    }

 done:
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPermObj);

    return ret;
}

int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime)
{
    // JAVA EQUIVALENT:
    //  fs.setTimes(src, mtime, atime)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
      fprintf(stderr, "could not construct path object\n");
      return -2;
    }

    jlong jmtime = mtime * (jlong)1000;
    jlong jatime = atime * (jlong)1000;

    int ret = 0;
    jthrowable jExc = NULL;
    if (invokeMethod(env, NULL, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "setTimes", JMETHOD3(JPARAM(HADOOP_PATH), "J", "J", JAVA_VOID),
                     jPath, jmtime, jatime) != 0) {
      fprintf(stderr, "call to setTime failed\n");
      errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                 "FileSystem::setTimes");
      ret = -1;
      goto done;
    }

 done:
    destroyLocalReference(env, jPath);
    return ret;
}




char***
hdfsGetHosts(hdfsFS fs, const char* path, tOffset start, tOffset length)
{
    // JAVA EQUIVALENT:
    //  fs.getFileBlockLoctions(new Path(path), start, length);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    jvalue jFSVal;
    jthrowable jFSExc = NULL;
    if (invokeMethod(env, &jFSVal, &jFSExc, INSTANCE, jFS,
                     HADOOP_FS, "getFileStatus", 
                     "(Lorg/apache/hadoop/fs/Path;)"
                     "Lorg/apache/hadoop/fs/FileStatus;",
                     jPath) != 0) {
        errno = errnoFromException(jFSExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::getFileStatus");
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jobject jFileStatus = jFSVal.l;

    //org.apache.hadoop.fs.FileSystem::getFileBlockLocations
    char*** blockHosts = NULL;
    jobjectArray jBlockLocations;;
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS,
                     HADOOP_FS, "getFileBlockLocations", 
                     "(Lorg/apache/hadoop/fs/FileStatus;JJ)"
                     "[Lorg/apache/hadoop/fs/BlockLocation;",
                     jFileStatus, start, length) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::getFileBlockLocations");
        destroyLocalReference(env, jPath);
        destroyLocalReference(env, jFileStatus);
        return NULL;
    }
    jBlockLocations = jVal.l;

    //Figure out no of entries in jBlockLocations
    //Allocate memory and add NULL at the end
    jsize jNumFileBlocks = (*env)->GetArrayLength(env, jBlockLocations);

    blockHosts = malloc(sizeof(char**) * (jNumFileBlocks+1));
    if (blockHosts == NULL) {
        errno = ENOMEM;
        goto done;
    }
    blockHosts[jNumFileBlocks] = NULL;
    if (jNumFileBlocks == 0) {
        errno = 0;
        goto done;
    }

    //Now parse each block to get hostnames
    int i = 0;
    for (i=0; i < jNumFileBlocks; ++i) {
        jobject jFileBlock =
            (*env)->GetObjectArrayElement(env, jBlockLocations, i);
        
        jvalue jVal;
        jobjectArray jFileBlockHosts;
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFileBlock, HADOOP_BLK_LOC,
                         "getHosts", "()[Ljava/lang/String;") ||
                jVal.l == NULL) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                       "BlockLocation::getHosts");
            destroyLocalReference(env, jPath);
            destroyLocalReference(env, jFileStatus);
            destroyLocalReference(env, jBlockLocations);
            return NULL;
        }
        
        jFileBlockHosts = jVal.l;
        //Figure out no of hosts in jFileBlockHosts
        //Allocate memory and add NULL at the end
        jsize jNumBlockHosts = (*env)->GetArrayLength(env, jFileBlockHosts);
        blockHosts[i] = malloc(sizeof(char*) * (jNumBlockHosts+1));
        if (blockHosts[i] == NULL) {
            int x = 0;
            for (x=0; x < i; ++x) {
                free(blockHosts[x]);
            }
            free(blockHosts);
            errno = ENOMEM;
            goto done;
        }
        blockHosts[i][jNumBlockHosts] = NULL;

        //Now parse each hostname
        int j = 0;
        const char *hostName;
        for (j=0; j < jNumBlockHosts; ++j) {
            jstring jHost =
                (*env)->GetObjectArrayElement(env, jFileBlockHosts, j);
           
            hostName =
                (const char*)((*env)->GetStringUTFChars(env, jHost, NULL));
            blockHosts[i][j] = strdup(hostName);

            (*env)->ReleaseStringUTFChars(env, jHost, hostName);
            destroyLocalReference(env, jHost);
        }

        destroyLocalReference(env, jFileBlockHosts);
    }
  
    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jFileStatus);
    destroyLocalReference(env, jBlockLocations);

    return blockHosts;
}


void hdfsFreeHosts(char ***blockHosts)
{
    int i, j;
    for (i=0; blockHosts[i]; i++) {
        for (j=0; blockHosts[i][j]; j++) {
            free(blockHosts[i][j]);
        }
        free(blockHosts[i]);
    }
    free(blockHosts);
}


tOffset hdfsGetDefaultBlockSize(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.getDefaultBlockSize();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //FileSystem::getDefaultBlockSize()
    tOffset blockSize = -1;
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "getDefaultBlockSize", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::getDefaultBlockSize");
        return -1;
    }
    blockSize = jVal.j;

    return blockSize;
}



tOffset hdfsGetCapacity(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  FsStatus fss = fs.getStatus();
    //  return Fss.getCapacity();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //FileSystem::getStatus
    jvalue  jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "getStatus", "()Lorg/apache/hadoop/fs/FsStatus;") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::getStatus");
        return -1;
    }
    jobject fss = (jobject)jVal.l;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, fss, HADOOP_FSSTATUS,
                     "getCapacity", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FsStatus::getCapacity");
        return -1;
    }
    return jVal.j;
}


  
tOffset hdfsGetUsed(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  FsStatus fss = fs.getStatus();
    //  return Fss.getUsed();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jFS = (jobject)fs;

    //FileSystem::getStatus
    jvalue  jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "getStatus", "()Lorg/apache/hadoop/fs/FsStatus;") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::getStatus");
        return -1;
    }
    jobject fss = (jobject)jVal.l;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, fss, HADOOP_FSSTATUS,
                     "getUsed", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FsStatus::getUsed");
        return -1;
    }
    return jVal.j;

}


 
static int
getFileInfoFromStat(JNIEnv *env, jobject jStat, hdfsFileInfo *fileInfo)
{
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat,
                     HADOOP_STAT, "isDir", "()Z") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileStatus::isDir");
        return -1;
    }
    fileInfo->mKind = jVal.z ? kObjectKindDirectory : kObjectKindFile;

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat,
                     HADOOP_STAT, "getReplication", "()S") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileStatus::getReplication");
        return -1;
    }
    fileInfo->mReplication = jVal.s;

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat,
                     HADOOP_STAT, "getBlockSize", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileStatus::getBlockSize");
        return -1;
    }
    fileInfo->mBlockSize = jVal.j;

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat,
                     HADOOP_STAT, "getModificationTime", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileStatus::getModificationTime");
        return -1;
    }
    fileInfo->mLastMod = (tTime) (jVal.j / 1000);

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat,
                     HADOOP_STAT, "getAccessTime", "()J") != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileStatus::getAccessTime");
        return -1;
    }
    fileInfo->mLastAccess = (tTime) (jVal.j / 1000);


    if (fileInfo->mKind == kObjectKindFile) {
        if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat,
                         HADOOP_STAT, "getLen", "()J") != 0) {
            errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                       "FileStatus::getLen");
            return -1;
        }
        fileInfo->mSize = jVal.j;
    }

    jobject jPath;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat, HADOOP_STAT,
                     "getPath", "()Lorg/apache/hadoop/fs/Path;") ||
            jVal.l == NULL) { 
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "Path::getPath");
        return -1;
    }
    jPath = jVal.l;

    jstring     jPathName;
    const char *cPathName;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jPath, HADOOP_PATH,
                     "toString", "()Ljava/lang/String;")) { 
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "Path::toString");
        destroyLocalReference(env, jPath);
        return -1;
    }
    jPathName = jVal.l;
    cPathName = (const char*) ((*env)->GetStringUTFChars(env, jPathName, NULL));
    fileInfo->mName = strdup(cPathName);
    (*env)->ReleaseStringUTFChars(env, jPathName, cPathName);
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPathName);
    jstring     jUserName;
    const char* cUserName;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat, HADOOP_STAT,
                    "getOwner", "()Ljava/lang/String;")) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileStatus::getOwner failed!\n");
        errno = EINTERNAL;
        return -1;
    }
    jUserName = jVal.l;
    cUserName = (const char*) ((*env)->GetStringUTFChars(env, jUserName, NULL));
    fileInfo->mOwner = strdup(cUserName);
    (*env)->ReleaseStringUTFChars(env, jUserName, cUserName);
    destroyLocalReference(env, jUserName);

    jstring     jGroupName;
    const char* cGroupName;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat, HADOOP_STAT,
                    "getGroup", "()Ljava/lang/String;")) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileStatus::getGroup failed!\n");
        errno = EINTERNAL;
        return -1;
    }
    jGroupName = jVal.l;
    cGroupName = (const char*) ((*env)->GetStringUTFChars(env, jGroupName, NULL));
    fileInfo->mGroup = strdup(cGroupName);
    (*env)->ReleaseStringUTFChars(env, jGroupName, cGroupName);
    destroyLocalReference(env, jGroupName);

    jobject jPermission;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jStat, HADOOP_STAT,
                     "getPermission", "()Lorg/apache/hadoop/fs/permission/FsPermission;") ||
            jVal.l == NULL) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileStatus::getPermission failed!\n");
        errno = EINTERNAL;
        return -1;
    }
    jPermission = jVal.l;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jPermission, HADOOP_FSPERM,
                         "toShort", "()S") != 0) {
            fprintf(stderr, "Call to org.apache.hadoop.fs.permission."
                    "FsPermission::toShort failed!\n");
            errno = EINTERNAL;
            return -1;
    }
    fileInfo->mPermissions = jVal.s;
    destroyLocalReference(env, jPermission);

    return 0;
}

static int
getFileInfo(JNIEnv *env, jobject jFS, jobject jPath, hdfsFileInfo *fileInfo)
{
    // JAVA EQUIVALENT:
    //  fs.isDirectory(f)
    //  fs.getModificationTime()
    //  fs.getAccessTime()
    //  fs.getLength(f)
    //  f.getPath()
    //  f.getOwner()
    //  f.getGroup()
    //  f.getPermission().toShort()

    jobject jStat;
    jvalue  jVal;
    jthrowable jExc = NULL;

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "exists", JMETHOD1(JPARAM(HADOOP_PATH), "Z"),
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::exists");
        return -1;
    }

    if (jVal.z == 0) {
      errno = ENOENT;
      return -1;
    }

    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_FS,
                     "getFileStatus", JMETHOD1(JPARAM(HADOOP_PATH), JPARAM(HADOOP_STAT)),
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::getFileStatus");
        return -1;
    }
    jStat = jVal.l;
    int ret =  getFileInfoFromStat(env, jStat, fileInfo); 
    destroyLocalReference(env, jStat);
    return ret;
}



hdfsFileInfo* hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries)
{
    // JAVA EQUIVALENT:
    //  Path p(path);
    //  Path []pathList = fs.listPaths(p)
    //  foreach path in pathList 
    //    getFileInfo(path)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    hdfsFileInfo *pathList = 0; 

    jobjectArray jPathList = NULL;
    jvalue jVal;
    jthrowable jExc = NULL;
    if (invokeMethod(env, &jVal, &jExc, INSTANCE, jFS, HADOOP_DFS, "listStatus",
                     JMETHOD1(JPARAM(HADOOP_PATH), JARRPARAM(HADOOP_STAT)),
                     jPath) != 0) {
        errno = errnoFromException(jExc, env, "org.apache.hadoop.fs."
                                   "FileSystem::listStatus");
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jPathList = jVal.l;

    //Figure out no of entries in that directory
    jsize jPathListSize = (*env)->GetArrayLength(env, jPathList);
    *numEntries = jPathListSize;
    if (jPathListSize == 0) {
        errno = 0;
        goto done;
    }

    //Allocate memory
    pathList = calloc(jPathListSize, sizeof(hdfsFileInfo));
    if (pathList == NULL) {
        errno = ENOMEM;
        goto done;
    }

    //Save path information in pathList
    jsize i;
    jobject tmpStat;
    for (i=0; i < jPathListSize; ++i) {
        tmpStat = (*env)->GetObjectArrayElement(env, jPathList, i);
        if (getFileInfoFromStat(env, tmpStat, &pathList[i])) {
            hdfsFreeFileInfo(pathList, jPathListSize);
            destroyLocalReference(env, tmpStat);
            pathList = NULL;
            goto done;
        }
        destroyLocalReference(env, tmpStat);
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPathList);

    return pathList;
}



hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  File f(path);
    //  fs.isDirectory(f)
    //  fs.lastModified() ??
    //  fs.getLength(f)
    //  f.getPath()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    hdfsFileInfo *fileInfo = calloc(1, sizeof(hdfsFileInfo));
    if (getFileInfo(env, jFS, jPath, fileInfo)) {
        hdfsFreeFileInfo(fileInfo, 1);
        fileInfo = NULL;
        goto done;
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return fileInfo;
}



void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries)
{
    //Free the mName, mOwner, and mGroup
    int i;
    for (i=0; i < numEntries; ++i) {
        if (hdfsFileInfo[i].mName) {
            free(hdfsFileInfo[i].mName);
        }
        if (hdfsFileInfo[i].mOwner) {
            free(hdfsFileInfo[i].mOwner);
        }
        if (hdfsFileInfo[i].mGroup) {
            free(hdfsFileInfo[i].mGroup);
        }
    }

    //Free entire block
    free(hdfsFileInfo);
}




/**
 * vim: ts=4: sw=4: et:
 */
