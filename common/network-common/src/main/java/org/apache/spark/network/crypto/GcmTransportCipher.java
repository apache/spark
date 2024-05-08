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
import com.google.crypto.tink.subtle.AesGcmJce;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import org.apache.spark.network.util.AbstractFileRegion;
import io.netty.buffer.ByteBuf;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.security.GeneralSecurityException;

public class GcmTransportCipher implements TransportCipher {
    private static final byte[] DEFAULT_AAD = new byte[0];

    private final SecretKeySpec aesKey;
    public GcmTransportCipher(SecretKeySpec aesKey)  {
        this.aesKey = aesKey;
    }

    @VisibleForTesting
    EncryptionHandler getEncryptionHandler() {
        return new EncryptionHandler();
    }

    @VisibleForTesting
    DecryptionHandler getDecryptionHandler() {
        return new DecryptionHandler();
    }

    public void addToChannel(Channel ch) {
        ch.pipeline()
            .addFirst("GcmTransportEncryption", getEncryptionHandler())
            .addFirst("GcmTransportDecryption", getDecryptionHandler());
    }

    @VisibleForTesting
    class EncryptionHandler extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                throws Exception {
            ByteBuffer inputBuffer;
            int bytesToRead;
            if (msg instanceof ByteBuf byteBuf) {
                bytesToRead = byteBuf.readableBytes();
                // This is allocating a buffer that is the size of the input
                inputBuffer = ByteBuffer.allocate(bytesToRead);
                // This will block while copying
                while (inputBuffer.position() < bytesToRead) {
                    byteBuf.readBytes(inputBuffer);
                }
            } else if (msg instanceof AbstractFileRegion fileRegion) {
                bytesToRead = (int) fileRegion.count();
                // This is allocating a buffer that is the size of the input
                inputBuffer = ByteBuffer.allocate(bytesToRead);
                ByteBufferWriteableChannel writeableChannel =
                        new ByteBufferWriteableChannel(inputBuffer);
                long transferred = 0;
                // This will block while copying
                while (transferred < bytesToRead) {
                    transferred +=
                            fileRegion.transferTo(writeableChannel, fileRegion.transferred());
                }
            } else {
                throw new IllegalArgumentException("Unsupported message type: " + msg.getClass());
            }
            AesGcmJce cipher = new AesGcmJce(aesKey.getEncoded());
            byte[] encrypted = cipher.encrypt(inputBuffer.array(), DEFAULT_AAD);
            ByteBuf wrappedEncrypted = Unpooled.wrappedBuffer(encrypted);
            ctx.write(wrappedEncrypted, promise);
        }
    }

    @VisibleForTesting
    class DecryptionHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws GeneralSecurityException {
            if (msg instanceof ByteBuf byteBuf) {
                // This is allocating a buffer that is the size of the input
                byte[] encrypted = new byte[byteBuf.readableBytes()];
                byteBuf.readBytes(encrypted);
                AesGcmJce cipher = new AesGcmJce(aesKey.getEncoded());
                byte[] decrypted = cipher.decrypt(encrypted, DEFAULT_AAD);
                ByteBuf wrappedDecrypted = Unpooled.wrappedBuffer(decrypted);
                ctx.fireChannelRead(wrappedDecrypted);
            } else {
                throw new IllegalArgumentException("Unsupported message type: " + msg.getClass());
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
            if (bytesToWrite == 0) {
                return 0; // Destination buffer is full
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
