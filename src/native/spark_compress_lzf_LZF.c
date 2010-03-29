#include "spark_compress_lzf_LZF.h"
#include <lzf.h>


/* Helper function to throw an exception */
static void throwException(JNIEnv *env, const char* className) {
  jclass cls = (*env)->FindClass(env, className);
  if (cls != 0) /* If cls is null, an exception was already thrown */
    (*env)->ThrowNew(env, cls, "");
}


/* 
 * Since LZF.compress() and LZF.decompress() have the same signatures
 * and differ only in which lzf_ function they call, implement both in a
 * single function and pass it a pointer to the correct lzf_ function.
 */
static jint callCompressionFunction
  (unsigned int (*func)(const void *const, unsigned int, void *, unsigned int),
   JNIEnv *env, jclass cls, jbyteArray inArray, jint inOff, jint inLen,
   jbyteArray outArray, jint outOff, jint outLen)
{
  jint inCap;
  jint outCap;
  jbyte *inData = 0;
  jbyte *outData = 0;
  jint ret;
  jint s;

  if (!inArray || !outArray) {
    throwException(env, "java/lang/NullPointerException");
    goto cleanup;
  }

  inCap = (*env)->GetArrayLength(env, inArray);
  outCap = (*env)->GetArrayLength(env, outArray);

  // Check if any of the offset/length pairs is invalid; we do this by OR'ing
  // things we don't want to be negative and seeing if the result is negative
  s = inOff | inLen | (inOff + inLen) | (inCap - (inOff + inLen)) |
      outOff | outLen | (outOff + outLen) | (outCap - (outOff + outLen));
  if (s < 0) {
    throwException(env, "java/lang/IndexOutOfBoundsException");
    goto cleanup;
  }

  inData = (*env)->GetPrimitiveArrayCritical(env, inArray, 0);
  outData = (*env)->GetPrimitiveArrayCritical(env, outArray, 0);

  if (!inData || !outData) {
    // Out of memory - JVM will throw OutOfMemoryError
    goto cleanup;
  }
  
  ret = func(inData + inOff, inLen, outData + outOff, outLen);

cleanup:
  if (inData)
    (*env)->ReleasePrimitiveArrayCritical(env, inArray, inData, 0);
  if (outData)
    (*env)->ReleasePrimitiveArrayCritical(env, outArray, outData, 0);
  
  return ret;
}

/*
 * Class:     spark_compress_lzf_LZF
 * Method:    compress
 * Signature: ([B[B)I
 */
JNIEXPORT jint JNICALL Java_spark_compress_lzf_LZF_compress
  (JNIEnv *env, jclass cls, jbyteArray inArray, jint inOff, jint inLen,
   jbyteArray outArray, jint outOff, jint outLen)
{
  return callCompressionFunction(lzf_compress, env, cls, 
      inArray, inOff, inLen, outArray,outOff, outLen);
}

/*
 * Class:     spark_compress_lzf_LZF
 * Method:    decompress
 * Signature: ([B[B)I
 */
JNIEXPORT jint JNICALL Java_spark_compress_lzf_LZF_decompress
  (JNIEnv *env, jclass cls, jbyteArray inArray, jint inOff, jint inLen,
   jbyteArray outArray, jint outOff, jint outLen)
{
  return callCompressionFunction(lzf_decompress, env, cls, 
      inArray, inOff, inLen, outArray,outOff, outLen);
}
