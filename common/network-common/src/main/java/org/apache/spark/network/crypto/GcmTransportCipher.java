/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.crypto;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import com.google.crypto.tink.subtle.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.ReferenceCounted;

import org.apache.spark.network.util.AbstractFileRegion;
import org.apache.spark.network.util.ByteBufferWriteableChannel;
import org.apache.spark.network.util.JavaUtils;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;

public class GcmTransportCipher implements TransportCipher {
    private static final String HKDF_ALG = "HmacSha256";
    private static final int LENGTH_HEADER_BYTES = 8;
    @VisibleForTesting
    static final int CIPHERTEXT_BUFFER_SIZE = 32 * 1024; // 32KB
    private final SecretKeySpec aesKey;

    public GcmTransportCipher(SecretKeySpec aesKey)  {
        this.aesKey = aesKey;
    }

    AesGcmHkdfStreaming getAesGcmHkdfStreaming() throws InvalidAlgorithmParameterException {
        return new AesGcmHkdfStreaming(
            aesKey.getEncoded(),
            HKDF_ALG,
            aesKey.getEncoded().length,
            CIPHERTEXT_BUFFER_SIZE,
            0);
    }

    /*
     * This method is for testing purposes only.
     */
    @VisibleForTesting
    public String getKeyId() throws GeneralSecurityException {
        return TransportCipherUtil.getKeyId(aesKey);
    }

    @VisibleForTesting
    EncryptionHandler getEncryptionHandler() throws GeneralSecurityException {
        return new EncryptionHandler();
    }

    @VisibleForTesting
    DecryptionHandler getDecryptionHandler() throws GeneralSecurityException {
        return new DecryptionHandler();
    }

    public void addToChannel(Channel ch) throws GeneralSecurityException {
        ch.pipeline()
            .addFirst("GcmTransportEncryption", getEncryptionHandler())
            .addFirst("GcmTransportDecryption", getDecryptionHandler());
    }

    @VisibleForTesting
    class EncryptionHandler extends ChannelOutboundHandlerAdapter {
        private final AesGcmHkdfStreaming aesGcmHkdfStreaming;

        EncryptionHandler() throws InvalidAlgorithmParameterException {
            aesGcmHkdfStreaming = getAesGcmHkdfStreaming();
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                throws Exception {
            ctx.write(new GcmEncryptedMessage(aesGcmHkdfStreaming, msg), promise);
        }
    }

    static class GcmEncryptedMessage extends AbstractFileRegion {
        private final Object plaintextMessage;
        private final ByteBuffer plaintextBuffer;
        private final ByteBuffer ciphertextBuffer;
        private final ByteBuffer headerByteBuffer;
        private final long bytesToRead;
        private long bytesRead = 0;
        private final StreamSegmentEncrypter encrypter;
        private long transferred = 0;
        private final long encryptedCount;

        GcmEncryptedMessage(AesGcmHkdfStreaming aesGcmHkdfStreaming,
                            Object plaintextMessage) throws GeneralSecurityException {
            JavaUtils.checkArgument(
                    plaintextMessage instanceof ByteBuf || plaintextMessage instanceof FileRegion,
                    "Unrecognized message type: %s", plaintextMessage.getClass().getName());
            this.plaintextMessage = plaintextMessage;
            this.plaintextBuffer =
                ByteBuffer.allocate(aesGcmHkdfStreaming.getPlaintextSegmentSize());
            this.ciphertextBuffer =
                ByteBuffer.allocate(aesGcmHkdfStreaming.getCiphertextSegmentSize());
            // If the ciphertext buffer cannot be fully written the target, transferTo may
            // return with it containing some unwritten data. The initial call we'll explicitly
            // set its limit to 0 to indicate the first call to transferTo.
            this.ciphertextBuffer.limit(0);

            this.bytesToRead = getReadableBytes();
            this.encryptedCount =
                    LENGTH_HEADER_BYTES + aesGcmHkdfStreaming.expectedCiphertextSize(bytesToRead);
            byte[] lengthAad = Longs.toByteArray(encryptedCount);
            this.encrypter = aesGcmHkdfStreaming.newStreamSegmentEncrypter(lengthAad);
            this.headerByteBuffer = createHeaderByteBuffer();
        }

        // The format of the output is:
        // [8 byte length][Internal IV and header][Ciphertext][Auth Tag]
        private ByteBuffer createHeaderByteBuffer() {
            ByteBuffer encrypterHeader = encrypter.getHeader();
            return ByteBuffer
                    .allocate(encrypterHeader.remaining() + LENGTH_HEADER_BYTES)
                    .putLong(encryptedCount)
                    .put(encrypterHeader)
                    .flip();
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public long transferred() {
            return transferred;
        }

        @Override
        public long count() {
            return encryptedCount;
        }

        @Override
        public GcmEncryptedMessage touch(Object o) {
            super.touch(o);
            if (plaintextMessage instanceof ByteBuf byteBuf) {
                byteBuf.touch(o);
            } else if (plaintextMessage instanceof FileRegion fileRegion) {
                fileRegion.touch(o);
            }
            return this;
        }

        @Override
        public GcmEncryptedMessage retain(int increment) {
            super.retain(increment);
            if (plaintextMessage instanceof ByteBuf byteBuf) {
                byteBuf.retain(increment);
            } else if (plaintextMessage instanceof FileRegion fileRegion) {
                fileRegion.retain(increment);
            }
            return this;
        }

        @Override
        public boolean release(int decrement) {
            if (plaintextMessage instanceof ByteBuf byteBuf) {
                byteBuf.release(decrement);
            } else if (plaintextMessage instanceof FileRegion fileRegion) {
                fileRegion.release(decrement);
            }
            return super.release(decrement);
        }

        @Override
        public long transferTo(WritableByteChannel target, long position) throws IOException {
            int transferredThisCall = 0;
            // If the header has is not empty, try to write it out to the target.
            if (headerByteBuffer.hasRemaining()) {
                int written = target.write(headerByteBuffer);
                transferredThisCall += written;
                this.transferred += written;
                if (headerByteBuffer.hasRemaining()) {
                    return written;
                }
            }
            // If the ciphertext buffer is not empty, try to write it to the target.
            if (ciphertextBuffer.hasRemaining()) {
                int written = target.write(ciphertextBuffer);
                transferredThisCall += written;
                this.transferred += written;
                if (ciphertextBuffer.hasRemaining()) {
                    return transferredThisCall;
               }
            }
            while (bytesRead < bytesToRead) {
                long readableBytes = getReadableBytes();
                int readLimit =
                        (int) Math.min(readableBytes, plaintextBuffer.remaining());
                if (plaintextMessage instanceof ByteBuf byteBuf) {
                    JavaUtils.checkState(0 == plaintextBuffer.position(),
                      "plaintextBuffer.position is not 0");
                    plaintextBuffer.limit(readLimit);
                    byteBuf.readBytes(plaintextBuffer);
                    JavaUtils.checkState(readLimit == plaintextBuffer.position(),
                      "plaintextBuffer.position should be equal to readLimit.");
                } else if (plaintextMessage instanceof FileRegion fileRegion) {
                    ByteBufferWriteableChannel plaintextChannel =
                            new ByteBufferWriteableChannel(plaintextBuffer);
                    long plaintextRead =
                            fileRegion.transferTo(plaintextChannel, fileRegion.transferred());
                    if (plaintextRead < readLimit) {
                        // If we do not read a full plaintext buffer or all the available
                        // readable bytes, return what was transferred this call.
                        return transferredThisCall;
                    }
                }
                boolean lastSegment = getReadableBytes() == 0;
                plaintextBuffer.flip();
                bytesRead += plaintextBuffer.remaining();
                ciphertextBuffer.clear();
                try {
                    encrypter.encryptSegment(plaintextBuffer, lastSegment, ciphertextBuffer);
                } catch (GeneralSecurityException e) {
                    throw new IllegalStateException("GeneralSecurityException from encrypter", e);
                }
                plaintextBuffer.clear();
                ciphertextBuffer.flip();
                int written = target.write(ciphertextBuffer);
                transferredThisCall += written;
                this.transferred += written;
                if (ciphertextBuffer.hasRemaining()) {
                    // In this case, upon calling transferTo again, it will try to write the
                    // remaining ciphertext buffer in the conditional before this loop.
                    return transferredThisCall;
                }
            }
            return transferredThisCall;
        }

        private long getReadableBytes() {
            if (plaintextMessage instanceof ByteBuf byteBuf) {
                return byteBuf.readableBytes();
            } else if (plaintextMessage instanceof FileRegion fileRegion) {
                return fileRegion.count() - fileRegion.transferred();
            } else {
                throw new IllegalArgumentException("Unsupported message type: " +
                        plaintextMessage.getClass().getName());
            }
        }

        @Override
        protected void deallocate() {
            if (plaintextMessage instanceof ReferenceCounted referenceCounted) {
                referenceCounted.release();
            }
            plaintextBuffer.clear();
            ciphertextBuffer.clear();
        }
    }

    @VisibleForTesting
    class DecryptionHandler extends ChannelInboundHandlerAdapter {
        private final ByteBuffer expectedLengthBuffer;
        private final ByteBuffer headerBuffer;
        private final ByteBuffer ciphertextBuffer;
        private final AesGcmHkdfStreaming aesGcmHkdfStreaming;
        private StreamSegmentDecrypter decrypter;
        private final int headerLength;
        private final int plaintextSegmentSize;
        private boolean decrypterInit = false;
        private boolean completed = false;
        private int segmentNumber = 0;
        private long expectedLength = -1;
        private long ciphertextRead = 0;

        DecryptionHandler() throws GeneralSecurityException {
            aesGcmHkdfStreaming = getAesGcmHkdfStreaming();
            headerLength = aesGcmHkdfStreaming.getHeaderLength();
            expectedLengthBuffer = ByteBuffer.allocate(LENGTH_HEADER_BYTES);
            headerBuffer = ByteBuffer.allocate(headerLength);
            ciphertextBuffer =
                    ByteBuffer.allocate(aesGcmHkdfStreaming.getCiphertextSegmentSize());
            decrypter = aesGcmHkdfStreaming.newStreamSegmentDecrypter();
            plaintextSegmentSize = aesGcmHkdfStreaming.getPlaintextSegmentSize();
        }

        /**
         * Resets all per-message state so that the next incoming GCM message can be
         * decoded through the same channel handler instance. This must be called after
         * every successfully completed message because AesGcmHkdfStreaming is a one-shot
         * streaming primitive: each encrypted message carries its own random IV and must
         * be decrypted with a fresh StreamSegmentDecrypter.
         */
        private void resetForNextMessage() throws GeneralSecurityException {
            expectedLength = -1;
            expectedLengthBuffer.clear();
            headerBuffer.clear();
            ciphertextBuffer.clear();
            decrypterInit = false;
            completed = false;
            segmentNumber = 0;
            ciphertextRead = 0;
            decrypter = aesGcmHkdfStreaming.newStreamSegmentDecrypter();
        }

        private boolean initializeExpectedLength(ByteBuf ciphertextNettyBuf) {
            if (expectedLength < 0) {
                // ByteBuf.readBytes(ByteBuffer) throws if fewer than dst.remaining() bytes
                // are available, so temporarily narrow the limit to what is actually present.
                int toRead = Math.min(ciphertextNettyBuf.readableBytes(),
                        expectedLengthBuffer.remaining());
                if (toRead > 0) {
                    int savedLimit = expectedLengthBuffer.limit();
                    expectedLengthBuffer.limit(expectedLengthBuffer.position() + toRead);
                    ciphertextNettyBuf.readBytes(expectedLengthBuffer);
                    expectedLengthBuffer.limit(savedLimit);
                }
                if (expectedLengthBuffer.hasRemaining()) {
                    // We did not read enough bytes to initialize the expected length.
                    return false;
                }
                expectedLengthBuffer.flip();
                expectedLength = expectedLengthBuffer.getLong();
                if (expectedLength < 0) {
                    throw new IllegalStateException("Invalid expected ciphertext length.");
                }
                ciphertextRead += LENGTH_HEADER_BYTES;
            }
            return true;
        }

        private boolean initializeDecrypter(ByteBuf ciphertextNettyBuf)
                throws GeneralSecurityException {
            // Check if the ciphertext header has been read. This contains
            // the IV and other internal metadata.
            if (!decrypterInit) {
                // ByteBuf.readBytes(ByteBuffer) throws if fewer than dst.remaining() bytes
                // are available. Under TCP fragmentation the header can arrive in multiple
                // chunks, so temporarily narrow the limit to what is actually present.
                int toRead = Math.min(ciphertextNettyBuf.readableBytes(),
                        headerBuffer.remaining());
                if (toRead > 0) {
                    int savedLimit = headerBuffer.limit();
                    headerBuffer.limit(headerBuffer.position() + toRead);
                    ciphertextNettyBuf.readBytes(headerBuffer);
                    headerBuffer.limit(savedLimit);
                }
                if (headerBuffer.hasRemaining()) {
                    // We did not read enough bytes to initialize the header.
                    return false;
                }
                headerBuffer.flip();
                byte[] lengthAad = Longs.toByteArray(expectedLength);
                decrypter.init(headerBuffer, lengthAad);
                decrypterInit = true;
                ciphertextRead += headerLength;
                if (expectedLength == ciphertextRead) {
                    // If the expected length is just the header, the ciphertext is 0 length.
                    completed = true;
                }
            }
            return true;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object ciphertextMessage)
                throws GeneralSecurityException {
            JavaUtils.checkArgument(ciphertextMessage instanceof ByteBuf,
                    "Unrecognized message type: %s",
                    ciphertextMessage.getClass().getName());
            ByteBuf ciphertextNettyBuf = (ByteBuf) ciphertextMessage;
            // The format of each message is:
            // [8 byte length][Internal IV and header][Ciphertext][Auth Tag]
            //
            // A single channelRead() call may deliver bytes from multiple back-to-back
            // GCM messages (common under shuffle load when TCP coalesces writes). The
            // outer loop processes as many complete messages as possible from the buffer
            // before releasing it, so that bytes belonging to the next message are never
            // discarded mid-stream.
            try {
                while (true) {
                    if (!initializeExpectedLength(ciphertextNettyBuf)) {
                        // We have not read enough bytes to initialize the expected length.
                        break;
                    }
                    if (!initializeDecrypter(ciphertextNettyBuf)) {
                        // We have not read enough bytes to initialize a header, needed to
                        // initialize a decrypter.
                        break;
                    }
                    int nettyBufReadableBytes = ciphertextNettyBuf.readableBytes();
                    while (nettyBufReadableBytes > 0 && !completed) {
                        // Read the ciphertext into the local buffer
                        int readableBytes = Math.min(
                                nettyBufReadableBytes,
                                ciphertextBuffer.remaining());
                        int expectedRemaining = (int) (expectedLength - ciphertextRead);
                        int bytesToRead = Math.min(readableBytes, expectedRemaining);
                        // The smallest ciphertext size is 16 bytes for the auth tag
                        ciphertextBuffer.limit(ciphertextBuffer.position() + bytesToRead);
                        ciphertextNettyBuf.readBytes(ciphertextBuffer);
                        ciphertextRead += bytesToRead;
                        // Check if this is the last segment
                        if (ciphertextRead == expectedLength) {
                            completed = true;
                        } else if (ciphertextRead > expectedLength) {
                            throw new IllegalStateException("Read more ciphertext than expected.");
                        }
                        // If the ciphertext buffer is full, or this is the last segment,
                        // then decrypt it and fire a read.
                        if (ciphertextBuffer.limit() == ciphertextBuffer.capacity() || completed) {
                            ByteBuffer plaintextBuffer = ByteBuffer.allocate(plaintextSegmentSize);
                            ciphertextBuffer.flip();
                            decrypter.decryptSegment(
                                    ciphertextBuffer,
                                    segmentNumber,
                                    completed,
                                    plaintextBuffer);
                            segmentNumber++;
                            // Clear the ciphertext buffer because it's been read
                            ciphertextBuffer.clear();
                            plaintextBuffer.flip();
                            ctx.fireChannelRead(Unpooled.wrappedBuffer(plaintextBuffer));
                        } else {
                            // Set the ciphertext buffer up to read the next chunk
                            ciphertextBuffer.limit(ciphertextBuffer.capacity());
                        }
                        nettyBufReadableBytes = ciphertextNettyBuf.readableBytes();
                    }
                    if (!completed) {
                        // Partial message: more bytes needed from the next channelRead() call.
                        break;
                    }
                    // Current message is fully decoded. Reset state so the handler can
                    // decode the next independent GCM message on the same channel.
                    resetForNextMessage();
                    if (ciphertextNettyBuf.readableBytes() == 0) {
                        break;
                    }
                    // Remaining bytes may belong to another message; loop to process them.
                }
            } finally {
                ciphertextNettyBuf.release();
            }
        }
    }
}
