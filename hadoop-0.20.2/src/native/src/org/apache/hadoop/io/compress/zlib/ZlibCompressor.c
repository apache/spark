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

#if defined HAVE_CONFIG_H
  #include <config.h>
#endif

#if defined HAVE_STDIO_H
  #include <stdio.h>
#else
  #error 'stdio.h not found'
#endif  

#if defined HAVE_STDLIB_H
  #include <stdlib.h>
#else
  #error 'stdlib.h not found'
#endif  

#if defined HAVE_STRING_H
  #include <string.h>
#else
  #error 'string.h not found'
#endif  

#if defined HAVE_DLFCN_H
  #include <dlfcn.h>
#else
  #error 'dlfcn.h not found'
#endif  

#include "org_apache_hadoop_io_compress_zlib.h"
#include "org_apache_hadoop_io_compress_zlib_ZlibCompressor.h"

static jfieldID ZlibCompressor_clazz;
static jfieldID ZlibCompressor_stream;
static jfieldID ZlibCompressor_uncompressedDirectBuf;
static jfieldID ZlibCompressor_uncompressedDirectBufOff;
static jfieldID ZlibCompressor_uncompressedDirectBufLen;
static jfieldID ZlibCompressor_compressedDirectBuf;
static jfieldID ZlibCompressor_directBufferSize;
static jfieldID ZlibCompressor_finish;
static jfieldID ZlibCompressor_finished;

static int (*dlsym_deflateInit2_)(z_streamp, int, int, int, int, int, const char *, int);
static int (*dlsym_deflate)(z_streamp, int);
static int (*dlsym_deflateSetDictionary)(z_streamp, const Bytef *, uInt);
static int (*dlsym_deflateReset)(z_streamp);
static int (*dlsym_deflateEnd)(z_streamp);

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_initIDs(
	JNIEnv *env, jclass class
	) {
	// Load libz.so
	void *libz = dlopen(HADOOP_ZLIB_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
	if (!libz) {
		THROW(env, "java/lang/UnsatisfiedLinkError", "Cannot load libz.so");
	  	return;
	}

	// Locate the requisite symbols from libz.so
	dlerror();                                 // Clear any existing error
	LOAD_DYNAMIC_SYMBOL(dlsym_deflateInit2_, env, libz, "deflateInit2_");
	LOAD_DYNAMIC_SYMBOL(dlsym_deflate, env, libz, "deflate");
	LOAD_DYNAMIC_SYMBOL(dlsym_deflateSetDictionary, env, libz, "deflateSetDictionary");
	LOAD_DYNAMIC_SYMBOL(dlsym_deflateReset, env, libz, "deflateReset");
	LOAD_DYNAMIC_SYMBOL(dlsym_deflateEnd, env, libz, "deflateEnd");

	// Initialize the requisite fieldIds
    ZlibCompressor_clazz = (*env)->GetStaticFieldID(env, class, "clazz", 
                                                      "Ljava/lang/Class;");
    ZlibCompressor_stream = (*env)->GetFieldID(env, class, "stream", "J");
    ZlibCompressor_finish = (*env)->GetFieldID(env, class, "finish", "Z");
    ZlibCompressor_finished = (*env)->GetFieldID(env, class, "finished", "Z");
    ZlibCompressor_uncompressedDirectBuf = (*env)->GetFieldID(env, class, 
    									"uncompressedDirectBuf", 
    									"Ljava/nio/Buffer;");
    ZlibCompressor_uncompressedDirectBufOff = (*env)->GetFieldID(env, class, 
    										"uncompressedDirectBufOff", "I");
    ZlibCompressor_uncompressedDirectBufLen = (*env)->GetFieldID(env, class, 
    										"uncompressedDirectBufLen", "I");
    ZlibCompressor_compressedDirectBuf = (*env)->GetFieldID(env, class, 
    									"compressedDirectBuf", 
    									"Ljava/nio/Buffer;");
    ZlibCompressor_directBufferSize = (*env)->GetFieldID(env, class, 
    										"directBufferSize", "I");
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_init(
	JNIEnv *env, jclass class, jint level, jint strategy, jint windowBits
	) {
	// Create a z_stream
    z_stream *stream = malloc(sizeof(z_stream));
    if (!stream) {
		THROW(env, "java/lang/OutOfMemoryError", NULL);
		return (jlong)0;
    }
    memset((void*)stream, 0, sizeof(z_stream));

	// Initialize stream
	static const int memLevel = 8; 							// See zconf.h
    int rv = (*dlsym_deflateInit2_)(stream, level, Z_DEFLATED, windowBits,
    			memLevel, strategy, ZLIB_VERSION, sizeof(z_stream));
    			
    if (rv != Z_OK) {
	    // Contingency - Report error by throwing appropriate exceptions
	    free(stream);
	    stream = NULL;
	
		switch (rv) {
			case Z_MEM_ERROR: 
			    {
		    		THROW(env, "java/lang/OutOfMemoryError", NULL);
			    }
			break;
			case Z_STREAM_ERROR:
		    	{
		    		THROW(env, "java/lang/IllegalArgumentException", NULL);
		    	}
		    break;
			default:
		    	{
		    		THROW(env, "java/lang/InternalError", NULL);
		    	}
		    break;
	    }
	}
	
    return JLONG(stream);
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_ZlibCompressor_setDictionary(
	JNIEnv *env, jclass class, jlong stream, 
	jarray b, jint off, jint len
	) {
    Bytef *buf = (*env)->GetPrimitiveArrayCritical(env, b, 0);
    if (!buf) {
        return;
    }
    int rv = dlsym_deflateSetDictionary(ZSTREAM(stream), buf + off, len);
    (*env)->ReleasePrimitiveArrayCritical(env, b, buf, 0);
    
    if (rv != Z_OK) {
    	// Contingency - Report error by throwing appropriate exceptions
	    switch (rv) {
		    case Z_STREAM_ERROR:
			{	
		    	THROW(env, "java/lang/IllegalArgumentException", NULL);
			}
			break;
	    	default:
			{
				THROW(env, "java/lang/InternalError", (ZSTREAM(stream))->msg);
			}
			break;
	    }
    }
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_deflateBytesDirect(
	JNIEnv *env, jobject this
	) {
	// Get members of ZlibCompressor
    z_stream *stream = ZSTREAM(
    						(*env)->GetLongField(env, this, 
    									ZlibCompressor_stream)
    					);
    if (!stream) {
		THROW(env, "java/lang/NullPointerException", NULL);
		return (jint)0;
    } 

    // Get members of ZlibCompressor
    jobject clazz = (*env)->GetStaticObjectField(env, this, 
                                                 ZlibCompressor_clazz);
	jobject uncompressed_direct_buf = (*env)->GetObjectField(env, this, 
									ZlibCompressor_uncompressedDirectBuf);
	jint uncompressed_direct_buf_off = (*env)->GetIntField(env, this, 
									ZlibCompressor_uncompressedDirectBufOff);
	jint uncompressed_direct_buf_len = (*env)->GetIntField(env, this, 
									ZlibCompressor_uncompressedDirectBufLen);

	jobject compressed_direct_buf = (*env)->GetObjectField(env, this, 
									ZlibCompressor_compressedDirectBuf);
	jint compressed_direct_buf_len = (*env)->GetIntField(env, this, 
									ZlibCompressor_directBufferSize);

	jboolean finish = (*env)->GetBooleanField(env, this, ZlibCompressor_finish);

    // Get the input direct buffer
    LOCK_CLASS(env, clazz, "ZlibCompressor");
	Bytef* uncompressed_bytes = (*env)->GetDirectBufferAddress(env, 
											uncompressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "ZlibCompressor");
    
  	if (uncompressed_bytes == 0) {
    	return (jint)0;
	}
	
    // Get the output direct buffer
    LOCK_CLASS(env, clazz, "ZlibCompressor");
	Bytef* compressed_bytes = (*env)->GetDirectBufferAddress(env, 
										compressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "ZlibCompressor");

  	if (compressed_bytes == 0) {
		return (jint)0;
	}
	
	// Re-calibrate the z_stream
  	stream->next_in = uncompressed_bytes + uncompressed_direct_buf_off;
  	stream->next_out = compressed_bytes;
  	stream->avail_in = uncompressed_direct_buf_len;
	stream->avail_out = compressed_direct_buf_len;
	
	// Compress
	int rv = dlsym_deflate(stream, finish ? Z_FINISH : Z_NO_FLUSH);

	jint no_compressed_bytes = 0;
	switch (rv) {
    	// Contingency? - Report error by throwing appropriate exceptions
  		case Z_STREAM_END:
  		{
  			(*env)->SetBooleanField(env, this, ZlibCompressor_finished, JNI_TRUE);
  		} // cascade
	  	case Z_OK: 
	  	{
	  		uncompressed_direct_buf_off += uncompressed_direct_buf_len - stream->avail_in;
			(*env)->SetIntField(env, this, 
						ZlibCompressor_uncompressedDirectBufOff, uncompressed_direct_buf_off);
			(*env)->SetIntField(env, this, 
						ZlibCompressor_uncompressedDirectBufLen, stream->avail_in);
			no_compressed_bytes = compressed_direct_buf_len - stream->avail_out;
	  	}
	  	break;
  		case Z_BUF_ERROR:
  		break;
  		default:
		{
			THROW(env, "java/lang/InternalError", stream->msg);
		}
		break;
  	}
  	
  	return no_compressed_bytes;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_getBytesRead(
	JNIEnv *env, jclass class, jlong stream
	) {
    return (ZSTREAM(stream))->total_in;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_getBytesWritten(
	JNIEnv *env, jclass class, jlong stream
	) {
    return (ZSTREAM(stream))->total_out;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_reset(
	JNIEnv *env, jclass class, jlong stream
	) {
    if (dlsym_deflateReset(ZSTREAM(stream)) != Z_OK) {
		THROW(env, "java/lang/InternalError", NULL);
    }
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibCompressor_end(
	JNIEnv *env, jclass class, jlong stream
	) {
    if (dlsym_deflateEnd(ZSTREAM(stream)) == Z_STREAM_ERROR) {
		THROW(env, "java/lang/InternalError", NULL);
    } else {
		free(ZSTREAM(stream));
    }
}

/**
 * vim: sw=2: ts=2: et:
 */

