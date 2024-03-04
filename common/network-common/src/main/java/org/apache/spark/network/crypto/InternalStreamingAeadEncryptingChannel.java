package org.apache.spark.network.crypto;

////////////////////////////////////////////////////////////////////////////////
// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////

import com.google.crypto.tink.subtle.AesGcmHkdfStreaming;
import com.google.crypto.tink.subtle.StreamSegmentEncrypter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.security.GeneralSecurityException;

/**
 * This code is based on from Google Tink's StreamingAeadEncryptingChannel. We adapt it here because Tink does
 * not allow you to pass in a buffer and would allocate a new buffer on each run.
 */
class InternalStreamingAeadEncryptingChannel implements WritableByteChannel {
    private static final byte[] UNUSED_AAD = new byte[0];
    private final StreamSegmentEncrypter encrypter;
    private final ByteBuffer plaintextBuffer;
    private final ByteBuffer ciphertextBuffer;
    private WritableByteChannel ciphertextChannel;
    private boolean open = true;
    private long inputTransferred;
    long written;

    InternalStreamingAeadEncryptingChannel(AesGcmHkdfStreaming gcmStreamer,
                                           ByteBuffer plaintextBuffer,
                                           ByteBuffer ciphertextBuffer) throws GeneralSecurityException {
        this.encrypter = gcmStreamer.newStreamSegmentEncrypter(UNUSED_AAD);

        // Clear and limit the plaintext buffer
        this.plaintextBuffer = plaintextBuffer;
        this.plaintextBuffer.clear();
        int plaintextSegmentSize = gcmStreamer.getPlaintextSegmentSize();
        int ciphertextOffset = gcmStreamer.getCiphertextOffset();
        this.plaintextBuffer.limit( plaintextSegmentSize - ciphertextOffset);

        // Clear the ciphertext buffer
        this.ciphertextBuffer = ciphertextBuffer;
        this.ciphertextBuffer.clear();

        this.inputTransferred = 0;
    }

    @Override
    public int write(ByteBuffer plaintext) throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }
        int bytesWritten = 0;
        if (ciphertextBuffer.remaining() > 0) {
            bytesWritten += ciphertextChannel.write(ciphertextBuffer);
        }
        int startPosition = plaintext.position();
        while (plaintext.remaining() > plaintextBuffer.remaining()) {
            if (ciphertextBuffer.remaining() > 0) {
                return plaintext.position() - startPosition;
            }
            int sliceSize = plaintextBuffer.remaining();
            ByteBuffer slice = plaintext.slice();
            slice.limit(sliceSize);
            plaintext.position(plaintext.position() + sliceSize);
            try {
                plaintextBuffer.flip();
                ciphertextBuffer.clear();
                if (slice.remaining() != 0) {
                    encrypter.encryptSegment(plaintextBuffer, slice, false, ciphertextBuffer);
                } else {
                    encrypter.encryptSegment(plaintextBuffer, false, ciphertextBuffer);
                }
            } catch (GeneralSecurityException ex) {
                throw new IOException(ex);
            }
            ciphertextBuffer.flip();
            bytesWritten += ciphertextChannel.write(ciphertextBuffer);
            plaintextBuffer.clear();
            plaintextBuffer.limit(plaintextBuffer.capacity());
        }
        plaintextBuffer.put(plaintext);
        int t = (plaintext.position() - startPosition);
        inputTransferred += t;
        written += bytesWritten;
        return t;
    }

    /**
     * @return The total number of plaintext bytes read from a source to far
     */
    long getInputTransferred() { return inputTransferred;}

    /**
     * The total number of ciphertext bytes written so far. We expect this to be greater than the number transferred
     * since it will include a header and authentication tag.
     *
     * @return The total number of ciphertext bytes written so far.
     */
    long getWritten() { return written; }

    @Override
    public boolean isOpen() { return open;}

    @Override
    public void close() throws IOException {
        written += flushCiphertextBuffer(false);
        try {
            ciphertextBuffer.clear();
            plaintextBuffer.flip();
            encrypter.encryptSegment(plaintextBuffer, true, ciphertextBuffer);
        } catch (GeneralSecurityException ex) {
            throw new IOException(ex);
        }
        ciphertextBuffer.flip();
        written += flushCiphertextBuffer(true);
        ciphertextChannel.close();
        this.open = false;
    }

    private int flushCiphertextBuffer(boolean isLastSegment) throws IOException {
        int totalWritten = 0;
        while (ciphertextBuffer.remaining() > 0) {
            int bytesToWrite = ciphertextBuffer.remaining();
            int written = ciphertextChannel.write(ciphertextBuffer);
            totalWritten += written;
            if (written <= 0) {
                throw new IOException("Unable to write remaining " + bytesToWrite + " bytes of ciphertext " +
                        ((isLastSegment) ? "for final segment." : ".") + " Ensure output channel has capacity.");
            }
        }
        return totalWritten;
    }

    void setCiphertextChannel(WritableByteChannel target) throws IOException {
        if (ciphertextChannel == null) {
            ciphertextChannel = target;
            ciphertextBuffer.clear();
            // At this point, ciphertextChannel might not yet be ready to receive bytes.
            // Buffering the header in ciphertextBuffer ensures that the header will be written when writing to
            // ciphertextChannel is possible.
            ciphertextBuffer.put(encrypter.getHeader());
            ciphertextBuffer.flip();
            written += ciphertextChannel.write(ciphertextBuffer);
        }
    }
}



