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
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.crypto.tink.subtle.AesGcmHkdfStreaming;
import com.google.crypto.tink.subtle.StreamSegmentDecrypter;
import com.google.crypto.tink.subtle.StreamSegmentEncrypter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.ReferenceCounted;
import org.apache.spark.network.util.AbstractFileRegion;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
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
        private final ByteBuffer plaintextBuffer;
        private final ByteBuffer ciphertextBuffer;
        private final AesGcmHkdfStreaming aesGcmHkdfStreaming;

        EncryptionHandler() throws InvalidAlgorithmParameterException {
            aesGcmHkdfStreaming = getAesGcmHkdfStreaming();
            plaintextBuffer = ByteBuffer.allocate(aesGcmHkdfStreaming.getPlaintextSegmentSize());
            ciphertextBuffer = ByteBuffer.allocate(aesGcmHkdfStreaming.getCiphertextSegmentSize());
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                throws Exception {
            GcmEncryptedMessage encryptedMessage = new GcmEncryptedMessage(
                    aesGcmHkdfStreaming,
                    msg,
                    plaintextBuffer,
                    ciphertextBuffer);
            ctx.write(encryptedMessage, promise);
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
                            Object plaintextMessage,
                            ByteBuffer plaintextBuffer,
                            ByteBuffer ciphertextBuffer) throws GeneralSecurityException {
            Preconditions.checkArgument(
                    plaintextMessage instanceof ByteBuf || plaintextMessage instanceof FileRegion,
                    "Unrecognized message type: %s", plaintextMessage.getClass().getName());
            this.plaintextMessage = plaintextMessage;
            this.plaintextBuffer = plaintextBuffer;
            this.ciphertextBuffer = ciphertextBuffer;
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
            } else if (plaintextMessage instanceof AbstractFileRegion fileRegion) {
                fileRegion.touch(o);
            }
            return this;
        }

        @Override
        public GcmEncryptedMessage retain(int increment) {
            super.retain(increment);
            if (plaintextMessage instanceof ByteBuf byteBuf) {
                byteBuf.retain(increment);
            } else if (plaintextMessage instanceof AbstractFileRegion fileRegion) {
                fileRegion.retain(increment);
            }
            return this;
        }

        @Override
        public boolean release(int decrement) {
            if (plaintextMessage instanceof ByteBuf byteBuf) {
                byteBuf.release(decrement);
            } else if (plaintextMessage instanceof AbstractFileRegion fileRegion) {
                fileRegion.release(decrement);
            }
            return super.release(decrement);
        }

        @Override
        public long transferTo(WritableByteChannel target, long position) throws IOException {
            Preconditions.checkArgument(position == transferred(),
                    "Invalid position.");
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
                boolean lastSegment = readableBytes <= plaintextBuffer.capacity();
                plaintextBuffer.clear();
                int readLimit =
                        (int) Math.min(readableBytes, plaintextBuffer.capacity());
                plaintextBuffer.limit(readLimit);
                if (plaintextMessage instanceof ByteBuf byteBuf) {
                    byteBuf.readBytes(plaintextBuffer);
                    long inputBytesRead = readableBytes - byteBuf.readableBytes();
                    bytesRead += inputBytesRead;
                } else if (plaintextMessage instanceof AbstractFileRegion fileRegion) {
                    ByteBufferWriteableChannel plaintextChannel =
                            new ByteBufferWriteableChannel(plaintextBuffer);
                    long transferred =
                            fileRegion.transferTo(plaintextChannel, fileRegion.transferred());
                    bytesRead += transferred;
                    if (transferred == 0) {
                        // File regions may return 0 if they are not ready to transfer
                        // more data. In that case, we'll return with the expectation
                        // that this transferTo() is called again.
                        return transferredThisCall;
                    }
                }
                plaintextBuffer.flip();
                ciphertextBuffer.clear();
                try {
                    encrypter.encryptSegment(plaintextBuffer, lastSegment, ciphertextBuffer);
                } catch (GeneralSecurityException e) {
                    throw new RuntimeException(e);
                }
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
            } else if (plaintextMessage instanceof AbstractFileRegion fileRegion) {
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
        private final ByteBuffer ciphertextBuffer;
        private final ByteBuffer plaintextBuffer;
        private final AesGcmHkdfStreaming aesGcmHkdfStreaming;
        private final StreamSegmentDecrypter decrypter;
        private boolean decrypterInit = false;
        private int segmentNumber = 0;
        private long expectedLength = -1;
        private long ciphertextRead = 0;

        DecryptionHandler() throws GeneralSecurityException {
            aesGcmHkdfStreaming = getAesGcmHkdfStreaming();
            plaintextBuffer =
                    ByteBuffer.allocate(aesGcmHkdfStreaming.getPlaintextSegmentSize());
            ciphertextBuffer =
                    ByteBuffer.allocate(aesGcmHkdfStreaming.getCiphertextSegmentSize());
            decrypter = aesGcmHkdfStreaming.newStreamSegmentDecrypter();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object ciphertextMessage)
                throws GeneralSecurityException {
            Preconditions.checkArgument(ciphertextMessage instanceof ByteBuf,
                    "Unrecognized message type: %s",
                    ciphertextMessage.getClass().getName());
            ByteBuf ciphertextNettyBuf = (ByteBuf) ciphertextMessage;
            // The format of the output is:
            // [8 byte length][Internal IV and header][Ciphertext][Auth Tag]
            try {
                while (ciphertextNettyBuf.readableBytes() > 0) {
                    // Check if the expected ciphertext length has been read.
                    if (expectedLength < 0 &&
                            ciphertextNettyBuf.readableBytes() >= LENGTH_HEADER_BYTES) {
                        expectedLength = ciphertextNettyBuf.readLong();
                        if (expectedLength < 0) {
                            throw new IllegalStateException("Invalid expected ciphertext length.");
                        }
                        ciphertextRead += LENGTH_HEADER_BYTES;
                    }
                    int headerLength = aesGcmHkdfStreaming.getHeaderLength();
                    // Check if the ciphertext header has been read. This contains
                    // the IV and other internal metadata.
                    if (!decrypterInit &&
                            ciphertextNettyBuf.readableBytes() >= headerLength) {
                        ByteBuffer headerBuffer = ByteBuffer.allocate(headerLength);
                        ciphertextNettyBuf.readBytes(headerBuffer);
                        headerBuffer.flip();
                        byte[] lengthAad = Longs.toByteArray(expectedLength);
                        decrypter.init(headerBuffer, lengthAad);
                        decrypterInit = true;
                        ciphertextRead += headerLength;
                    }
                    // This may occur if there weren't enough readable bytes to read the header.
                    if (!decrypterInit) {
                        return;
                    }
                    // This may occur if the expected length is just the header.
                    if (expectedLength == ciphertextRead) {
                        return;
                    }
                    ciphertextBuffer.clear();
                    // Read the ciphertext into the local buffer
                    int readableBytes = Integer.min(
                            ciphertextNettyBuf.readableBytes(),
                            ciphertextBuffer.remaining());
                    if (readableBytes == 0) {
                        return;
                    }
                    int expectedRemaining = (int) (expectedLength - ciphertextRead);
                    int bytesToRead = Integer.min(readableBytes, expectedRemaining);
                    // The smallest ciphertext size is 16 bytes for the auth tag
                    ciphertextBuffer.limit(bytesToRead);
                    ciphertextNettyBuf.readBytes(ciphertextBuffer);
                    ciphertextRead += bytesToRead;
                    // Check if this is the last segment
                    boolean lastSegment = false;
                    if (ciphertextRead == expectedLength) {
                        lastSegment = true;
                    } else if (ciphertextRead > expectedLength) {
                        throw new IllegalStateException("Read more ciphertext than expected.");
                    }
                    plaintextBuffer.clear();
                    ciphertextBuffer.flip();

                    decrypter.decryptSegment(
                            ciphertextBuffer,
                            segmentNumber,
                            lastSegment,
                            plaintextBuffer);
                    segmentNumber++;
                    plaintextBuffer.flip();
                    ctx.fireChannelRead(Unpooled.wrappedBuffer(plaintextBuffer));
                }
            } finally {
                ciphertextNettyBuf.release();
            }
        }
    }

    static class ByteBufferWriteableChannel implements WritableByteChannel {
        private final ByteBuffer destination;
        private boolean open;

        ByteBufferWriteableChannel(ByteBuffer destination) {
            this.destination = destination;
            this.open = true;
        }
        @Override
        public int write(ByteBuffer src) throws IOException {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            int bytesToWrite = Math.min(src.remaining(), destination.remaining());
            // Destination buffer is full
            if (bytesToWrite == 0) {
                return 0;
            }
            ByteBuffer temp = src.slice().limit(bytesToWrite);
            destination.put(temp);
            src.position(src.position() + bytesToWrite);
            return bytesToWrite;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}
