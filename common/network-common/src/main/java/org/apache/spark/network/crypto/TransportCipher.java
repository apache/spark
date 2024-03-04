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
import com.google.crypto.tink.subtle.AesGcmHkdfStreaming;
import com.google.crypto.tink.subtle.Hex;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.ReferenceCounted;
import org.apache.spark.network.util.AbstractFileRegion;
import org.apache.spark.network.util.ByteArrayReadableChannel;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.GeneralSecurityException;

/**
 * Cipher for encryption and decryption.
 */
public class TransportCipher {
    @VisibleForTesting
    static final String ENCRYPTION_HANDLER_NAME = "TransportEncryption";
    @VisibleForTesting
    static final int PLAINTEXT_SEGMENT_SIZE_32K_BYTES = 1024 * 32;
    private static final String DECRYPTION_HANDLER_NAME = "TransportDecryption";
    private static final int SEGMENT_ENCRYPTER_OVERHEAD_BYTES = 40;
    private static final String MAC_ALGORITHM = "HMACSHA256";
    private final String identifier;
    private final AesGcmHkdfStreaming gcmStreamer;

    public TransportCipher(String identifier, SecretKeySpec derivedAesKey) throws GeneralSecurityException {
        this.identifier = identifier;
        gcmStreamer = new AesGcmHkdfStreaming(
                derivedAesKey.getEncoded(),
                MAC_ALGORITHM,
                derivedAesKey.getEncoded().length,
                PLAINTEXT_SEGMENT_SIZE_32K_BYTES + SEGMENT_ENCRYPTER_OVERHEAD_BYTES,
                0
        );
    }

    @Override
    public String toString() {
        return "TransportCipher(id=" + identifier + ")";
    }

    AesGcmHkdfStreaming getGcmStreamer() {
        return gcmStreamer;
    }

    public long expectedCiphertextSize(long plaintextSize) {
        return gcmStreamer.expectedCiphertextSize(plaintextSize);
    }

    /**
     * Add handlers to channel.
     *
     * @param ch the channel for adding handlers
     */
    public void addToChannel(Channel ch) throws GeneralSecurityException {
        ch.pipeline()
                .addFirst(ENCRYPTION_HANDLER_NAME, new EncryptionHandler(this))
                .addFirst(DECRYPTION_HANDLER_NAME, new DecryptionHandler(this));
    }

    @VisibleForTesting
    static class EncryptionHandler extends ChannelOutboundHandlerAdapter {
        private final ByteBuffer plaintextBuffer;
        private final ByteBuffer ciphertextBuffer;
        private final AesGcmHkdfStreaming gcmStreamer;

        EncryptionHandler(TransportCipher transportCipher) {
            this.gcmStreamer = transportCipher.getGcmStreamer();
            plaintextBuffer = ByteBuffer.allocate(PLAINTEXT_SEGMENT_SIZE_32K_BYTES);
            ciphertextBuffer =
                    ByteBuffer.allocate((int) gcmStreamer.expectedCiphertextSize(PLAINTEXT_SEGMENT_SIZE_32K_BYTES));
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            EncryptedMessage<?> emsg;
            if (msg instanceof ByteBuf byteBuf) {
                emsg = createEncryptedMessage(byteBuf);
            } else if (msg instanceof FileRegion fileRegion) {
                emsg = createEncryptedMessage(fileRegion);
            } else {
                throw new Exception("Input message is not ByteBuf or FileRegion");
            }
            ctx.write(emsg, promise);
        }

        @VisibleForTesting
        EncryptedMessage<ByteBuf> createEncryptedMessage(ByteBuf msg) throws GeneralSecurityException {
            return new EncryptedMessage<>(gcmStreamer, msg, plaintextBuffer, ciphertextBuffer);
        }

        @VisibleForTesting
        EncryptedMessage<FileRegion> createEncryptedMessage(FileRegion msg) throws GeneralSecurityException {
            return new EncryptedMessage<>(gcmStreamer, msg, plaintextBuffer, ciphertextBuffer);
        }
    }

    private static class DecryptionHandler extends ChannelInboundHandlerAdapter {
        private final InternalStreamingAeadDecryptingChannel decryptingChannel;
        private final ByteArrayReadableChannel ciphertextChannel;
        private static int count = 0;
        private int localCount;
        DecryptionHandler(TransportCipher transportCipher) throws GeneralSecurityException {
            count++;
            localCount = count;
            System.out.println("New DecryptionHandler? " + localCount);
            ByteBuffer plaintextBuffer = ByteBuffer.allocate(PLAINTEXT_SEGMENT_SIZE_32K_BYTES);
            ByteBuffer ciphertextBuffer = ByteBuffer.allocate((int)
                    transportCipher.getGcmStreamer().expectedCiphertextSize(PLAINTEXT_SEGMENT_SIZE_32K_BYTES));
            decryptingChannel = new InternalStreamingAeadDecryptingChannel(
                    transportCipher.getGcmStreamer(),
                    plaintextBuffer,
                    ciphertextBuffer);
            ciphertextChannel = new ByteArrayReadableChannel();
            decryptingChannel.setCiphertextChannel(ciphertextChannel);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
            ByteBuf buffer = (ByteBuf) data;
            try {
                ByteBuffer decryptedByteBuffer = ByteBuffer.allocate(buffer.readableBytes());
                int bytesRead = 0;
                do {
                    ciphertextChannel.feedData(buffer);
                    bytesRead += decryptingChannel.read(decryptedByteBuffer);
                    ByteBuffer slice = decryptedByteBuffer.slice();
                    slice.limit(bytesRead);
                    System.out.println("Bytes Read: " + bytesRead);
                    System.out.println(decryptingChannel);
                    System.out.println(Hex.encode(decryptedByteBuffer.array()));
                    ctx.fireChannelRead(Unpooled.wrappedBuffer(slice));
                    System.out.println(Hex.encode(decryptedByteBuffer.array()));
                } while (bytesRead > 0);
            } finally {
                buffer.release();
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            System.out.println("Channel read complete? Local count " + localCount + " Count "+ count + " " + decryptingChannel);
            //decryptingChannel.close();
            ctx.fireChannelReadComplete();
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            System.out.println("Handler removed? Local count " + localCount + " Count "+ count + " " + decryptingChannel);
            super.handlerRemoved(ctx);
        }
    }

    @VisibleForTesting
    static class EncryptedMessage<T extends ReferenceCounted> extends AbstractFileRegion {
        private final T plaintextMessage;
        private final long outputExpectedCount;
        private final long inputCount;
        private final ByteBuffer plaintextBuffer;
        private final ByteBuffer ciphertextBuffer;
        private final AesGcmHkdfStreaming gcmStreamer;
        private final InternalStreamingAeadEncryptingChannel encryptingChannel;


        EncryptedMessage(
                AesGcmHkdfStreaming gcmStreamer,
                T plaintextMessage,
                ByteBuffer plaintextBuffer,
                ByteBuffer ciphertextBuffer) throws GeneralSecurityException {
            Preconditions.checkArgument(plaintextMessage instanceof ByteBuf || plaintextMessage instanceof FileRegion,
                    "Unrecognized message type: %s", plaintextMessage.getClass().getName());
            this.gcmStreamer = gcmStreamer;
            this.plaintextBuffer = plaintextBuffer;
            this.ciphertextBuffer = ciphertextBuffer;
            this.plaintextMessage = plaintextMessage;
            encryptingChannel = new InternalStreamingAeadEncryptingChannel(gcmStreamer, plaintextBuffer, ciphertextBuffer);

            // Set the expected count of bytes to read and the count of bytes transferred
            if (plaintextMessage instanceof ByteBuf byteBuf) {
                inputCount = byteBuf.readableBytes();
            } else {
                inputCount = ((FileRegion) plaintextMessage).count();
            }
            outputExpectedCount = expectedCiphertextSize((int) inputCount);
        }

        public long expectedCiphertextSize(int plaintextSizeBytes) {
            return gcmStreamer.expectedCiphertextSize(plaintextSizeBytes);
        }



        @Override
        public long count() {
            return outputExpectedCount;
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public long transferred() {
            return encryptingChannel.getWritten();
        }

        @Override
        public EncryptedMessage<T> touch(Object o) {
            super.touch(o);
            plaintextMessage.touch(o);
            return this;
        }

        @Override
        public EncryptedMessage<T> retain(int increment) {
            super.retain(increment);
            plaintextMessage.retain(increment);
            return this;
        }

        @Override
        public boolean release(int decrement) {
            plaintextMessage.release(decrement);
            return super.release(decrement);
        }


        @Override
        protected void deallocate() {
            System.out.println("EncryptMessage.deallocating()");
            plaintextMessage.release();
            plaintextBuffer.clear();
            ciphertextBuffer.clear();
        }

        @Override
        public long transferTo(WritableByteChannel target, long position) throws IOException {
            Preconditions.checkArgument(position == transferred(), "Invalid position.");
            Preconditions.checkState(encryptingChannel.isOpen());
            if (encryptingChannel.getInputTransferred() == outputExpectedCount) {
                return 0;
            }
            encryptingChannel.setCiphertextChannel(target);
            do {
                if (plaintextMessage instanceof ByteBuf byteBuf) {
                    encryptingChannel.write(byteBuf.nioBuffer());
                } else if (plaintextMessage instanceof FileRegion fileRegion) {
                    // It's allowable for FileRegions to transfer 0 bytes. If they do, we just skip.
                    if (fileRegion.transferTo(encryptingChannel, fileRegion.transferred()) == 0) {
                        return 0;
                    }
                } else {
                  // This should not be reachable because we have preconditions in the constructor.
                  throw new RuntimeException("Plaintext Message is not a ByteBuf or FileRegion");
                }
            } while (encryptingChannel.getInputTransferred() < inputCount);
            encryptingChannel.close();
            return encryptingChannel.getWritten();
        }

        @Override
        public String toString() {
            return "EncryptedMessage [" +
                    "\ninputCount: " + inputCount +
                    "\noutputExpectedCount: " + outputExpectedCount +
                    "\nciphertextTransferred: " + encryptingChannel.getWritten() +
                    "\nChannel Open? " + encryptingChannel.isOpen() +
                    "\n]";
        }
    }
}