/*
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
#include "org_apache_hadoop_io_compress_zlib_ZlibDecompressor.h"

static jfieldID ZlibDecompressor_clazz;
static jfieldID ZlibDecompressor_stream;
static jfieldID ZlibDecompressor_compressedDirectBuf;
static jfieldID ZlibDecompressor_compressedDirectBufOff;
static jfieldID ZlibDecompressor_compressedDirectBufLen;
static jfieldID ZlibDecompressor_uncompressedDirectBuf;
static jfieldID ZlibDecompressor_directBufferSize;
static jfieldID ZlibDecompressor_needDict;
static jfieldID ZlibDecompressor_finished;

static int (*dlsym_inflateInit2_)(z_streamp, int, const char *, int);
static int (*dlsym_inflate)(z_streamp, int);
static int (*dlsym_inflateSetDictionary)(z_streamp, const Bytef *, uInt);
static int (*dlsym_inflateReset)(z_streamp);
static int (*dlsym_inflateEnd)(z_streamp);

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibDecompressor_initIDs(
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
	LOAD_DYNAMIC_SYMBOL(dlsym_inflateInit2_, env, libz, "inflateInit2_");
	LOAD_DYNAMIC_SYMBOL(dlsym_inflate, env, libz, "inflate");
	LOAD_DYNAMIC_SYMBOL(dlsym_inflateSetDictionary, env, libz, "inflateSetDictionary");
	LOAD_DYNAMIC_SYMBOL(dlsym_inflateReset, env, libz, "inflateReset");
	LOAD_DYNAMIC_SYMBOL(dlsym_inflateEnd, env, libz, "inflateEnd");

	// Initialize the requisite fieldIds
    ZlibDecompressor_clazz = (*env)->GetStaticFieldID(env, class, "clazz", 
                                                      "Ljava/lang/Class;");
    ZlibDecompressor_stream = (*env)->GetFieldID(env, class, "stream", "J");
    ZlibDecompressor_needDict = (*env)->GetFieldID(env, class, "needDict", "Z");
    ZlibDecompressor_finished = (*env)->GetFieldID(env, class, "finished", "Z");
    ZlibDecompressor_compressedDirectBuf = (*env)->GetFieldID(env, class, 
    											"compressedDirectBuf", 
    											"Ljava/nio/Buffer;");
    ZlibDecompressor_compressedDirectBufOff = (*env)->GetFieldID(env, class, 
    										"compressedDirectBufOff", "I");
    ZlibDecompressor_compressedDirectBufLen = (*env)->GetFieldID(env, class, 
    										"compressedDirectBufLen", "I");
    ZlibDecompressor_uncompressedDirectBuf = (*env)->GetFieldID(env, class, 
    											"uncompressedDirectBuf", 
    											"Ljava/nio/Buffer;");
    ZlibDecompressor_directBufferSize = (*env)->GetFieldID(env, class, 
    											"directBufferSize", "I");
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibDecompressor_init(
	JNIEnv *env, jclass cls, jint windowBits
	) {
    z_stream *stream = malloc(sizeof(z_stream));
    memset((void*)stream, 0, sizeof(z_stream));

    if (stream == 0) {
		THROW(env, "java/lang/OutOfMemoryError", NULL);
		return (jlong)0;
    } 
    
    int rv = dlsym_inflateInit2_(stream, windowBits, ZLIB_VERSION, sizeof(z_stream));

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
Java_org_apache_hadoop_io_compress_zlib_ZlibDecompressor_setDictionary(
	JNIEnv *env, jclass cls, jlong stream,
	jarray b, jint off, jint len
	) {
    Bytef *buf = (*env)->GetPrimitiveArrayCritical(env, b, 0);
    if (!buf) {
		THROW(env, "java/lang/InternalError", NULL);
        return;
    }
    int rv = dlsym_inflateSetDictionary(ZSTREAM(stream), buf + off, len);
    (*env)->ReleasePrimitiveArrayCritical(env, b, buf, 0);
    
    if (rv != Z_OK) {
	    // Contingency - Report error by throwing appropriate exceptions
		switch (rv) {
		    case Z_STREAM_ERROR:
	    	case Z_DATA_ERROR:
			{
				THROW(env, "java/lang/IllegalArgumentException", 
					(ZSTREAM(stream))->msg);
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
Java_org_apache_hadoop_io_compress_zlib_ZlibDecompressor_inflateBytesDirect(
	JNIEnv *env, jobject this
	) {
	// Get members of ZlibDecompressor
    z_stream *stream = ZSTREAM(
    						(*env)->GetLongField(env, this, 
    									ZlibDecompressor_stream)
    					);
    if (!stream) {
		THROW(env, "java/lang/NullPointerException", NULL);
		return (jint)0;
    } 

    // Get members of ZlibDecompressor
    jobject clazz = (*env)->GetStaticObjectField(env, this, 
                                                 ZlibDecompressor_clazz);
	jarray compressed_direct_buf = (jarray)(*env)->GetObjectField(env, this, 
											ZlibDecompressor_compressedDirectBuf);
	jint compressed_direct_buf_off = (*env)->GetIntField(env, this, 
									ZlibDecompressor_compressedDirectBufOff);
	jint compressed_direct_buf_len = (*env)->GetIntField(env, this, 
									ZlibDecompressor_compressedDirectBufLen);

	jarray uncompressed_direct_buf = (jarray)(*env)->GetObjectField(env, this, 
											ZlibDecompressor_uncompressedDirectBuf);
	jint uncompressed_direct_buf_len = (*env)->GetIntField(env, this, 
										ZlibDecompressor_directBufferSize);

    // Get the input direct buffer
    LOCK_CLASS(env, clazz, "ZlibDecompressor");
	Bytef *compressed_bytes = (*env)->GetDirectBufferAddress(env, 
										compressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "ZlibDecompressor");
    
	if (!compressed_bytes) {
	    return (jint)0;
	}
	
    // Get the output direct buffer
    LOCK_CLASS(env, clazz, "ZlibDecompressor");
	Bytef *uncompressed_bytes = (*env)->GetDirectBufferAddress(env, 
											uncompressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "ZlibDecompressor");

	if (!uncompressed_bytes) {
	    return (jint)0;
	}
	
	// Re-calibrate the z_stream
	stream->next_in  = compressed_bytes + compressed_direct_buf_off;
	stream->next_out = uncompressed_bytes;
	stream->avail_in  = compressed_direct_buf_len;
	stream->avail_out = uncompressed_direct_buf_len;
	
	// Decompress
	int rv = dlsym_inflate(stream, Z_PARTIAL_FLUSH);

	// Contingency? - Report error by throwing appropriate exceptions
	int no_decompressed_bytes = 0;	
	switch (rv) {
		case Z_STREAM_END:
		{
		    (*env)->SetBooleanField(env, this, ZlibDecompressor_finished, JNI_TRUE);
		} // cascade down
		case Z_OK:
		{
		    compressed_direct_buf_off += compressed_direct_buf_len - stream->avail_in;
		    (*env)->SetIntField(env, this, ZlibDecompressor_compressedDirectBufOff, 
		    			compressed_direct_buf_off);
		    (*env)->SetIntField(env, this, ZlibDecompressor_compressedDirectBufLen, 
		    			stream->avail_in);
		    no_decompressed_bytes = uncompressed_direct_buf_len - stream->avail_out;
		}
		break;
		case Z_NEED_DICT:
		{
		    (*env)->SetBooleanField(env, this, ZlibDecompressor_needDict, JNI_TRUE);
		    compressed_direct_buf_off += compressed_direct_buf_len - stream->avail_in;
		    (*env)->SetIntField(env, this, ZlibDecompressor_compressedDirectBufOff, 
		    			compressed_direct_buf_off);
		    (*env)->SetIntField(env, this, ZlibDecompressor_compressedDirectBufLen, 
		    			stream->avail_in);
		}
		break;
		case Z_BUF_ERROR:
		break;
		case Z_DATA_ERROR:
		{
		    THROW(env, "java/io/IOException", stream->msg);
		}
		break;
		case Z_MEM_ERROR:
		{
		    THROW(env, "java/lang/OutOfMemoryError", NULL);
		}
		break;
		default:
		{
		    THROW(env, "java/lang/InternalError", stream->msg);
		}
		break;
    }
    
    return no_decompressed_bytes;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibDecompressor_getBytesRead(
	JNIEnv *env, jclass cls, jlong stream
	) {
    return (ZSTREAM(stream))->total_in;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibDecompressor_getBytesWritten(
	JNIEnv *env, jclass cls, jlong stream
	) {
    return (ZSTREAM(stream))->total_out;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibDecompressor_reset(
	JNIEnv *env, jclass cls, jlong stream
	) {
    if (dlsym_inflateReset(ZSTREAM(stream)) != Z_OK) {
		THROW(env, "java/lang/InternalError", 0);
    }
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_zlib_ZlibDecompressor_end(
	JNIEnv *env, jclass cls, jlong stream
	) {
    if (dlsym_inflateEnd(ZSTREAM(stream)) == Z_STREAM_ERROR) {
		THROW(env, "java/lang/InternalError", 0);
    } else {
		free(ZSTREAM(stream));
    }
}

/**
 * vim: sw=2: ts=2: et:
 */

