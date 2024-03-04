package org.apache.spark.network.crypto;

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
import com.google.crypto.tink.subtle.StreamSegmentDecrypter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.GeneralSecurityException;


/**
 * This is adapted from Google Tink's StreamingAeadDecryptingChannel in order to allow callers to pass in pre-defined
 * plaintext and ciphertext buffers.
 */

/** An instance of {@link ReadableByteChannel} that returns the plaintext for some ciphertext. */
class InternalStreamingAeadDecryptingChannel implements ReadableByteChannel {
    /* The stream containing the ciphertext */
    private ReadableByteChannel ciphertextChannel;

    /**
     * A buffer containing ciphertext that has not yet been decrypted.
     * The limit of ciphertextSegment is set such that it can contain segment plus the first
     * character of the next segment. It is necessary to read a segment plus one more byte
     * to decrypt a segment, since the last segment of a ciphertext is encrypted differently.
     */
    private final ByteBuffer ciphertextSegment;

    /**
     * A buffer containing a plaintext segment.
     * The bytes in the range plaintexSegment.position() .. plaintextSegment.limit() - 1
     * are plaintext that have been decrypted but not yet read out of AesGcmInputStream.
     */
    private final ByteBuffer plaintextSegment;

    /* A buffer containg the header information from the ciphertext. */
    private final ByteBuffer header;

    /* Determines whether the header has been completely read. */
    private boolean headerRead;

    /* Indicates whether the end of this InputStream has been reached. */
    boolean endOfCiphertext;

    /* Indicates whether the end of the plaintext has been reached. */
    private boolean endOfPlaintext;

    /**
     * Indicates whether this stream is in a defined state.
     * Currently the state of this instance becomes undefined when
     * an authentication error has occurred.
     */
    private boolean definedState;

    /** The associated data that is authenticated with the ciphertext. */
    private final byte[] UNUSED_AAD = new byte[0];

    /**
     * The number of the current segment of ciphertext buffered in ciphertextSegment.
     */
    private int segmentNr;

    private final StreamSegmentDecrypter decrypter;
    private final int ciphertextSegmentSize;
    private final int firstCiphertextSegmentSize;

    public InternalStreamingAeadDecryptingChannel(
            AesGcmHkdfStreaming streamAead,
            ByteBuffer plaintextBuffer,
            ByteBuffer ciphertextBuffer)
            throws GeneralSecurityException {
        decrypter = streamAead.newStreamSegmentDecrypter();
        header = ByteBuffer.allocate(streamAead.getHeaderLength());

        // ciphertextSegment is one byte longer than a ciphertext segment,
        // so that the code can decide if the current segment is the last segment in the
        // stream.
        ciphertextSegmentSize = streamAead.getCiphertextSegmentSize();
        ciphertextSegment = ciphertextBuffer;
        ciphertextSegment.limit(0);
        firstCiphertextSegmentSize = ciphertextSegmentSize - streamAead.getCiphertextOffset();
        plaintextSegment = plaintextBuffer;
        plaintextSegment.limit(0);
        headerRead = false;
        endOfCiphertext = false;
        endOfPlaintext = false;
        segmentNr = 0;
        definedState = true;
    }

    // This must be set by the caller prior to reading any data.
    void setCiphertextChannel(ReadableByteChannel ciphertextChannel) {
        this.ciphertextChannel = ciphertextChannel;
    }

    /**
     * Reads some ciphertext.
     * @param buffer the destination for the ciphertext.
     * @throws IOException when an exception reading the ciphertext stream occurs.
     */
    private void readSomeCiphertext(ByteBuffer buffer) throws IOException {
        int read;
        do {
            read = ciphertextChannel.read(buffer);
        } while (read > 0 && buffer.remaining() > 0);
        if (read == -1) {
            endOfCiphertext = true;
        }
    }

    /**
     * Tries to read the header of the ciphertext.
     * @return true if the header has been fully read and false if not enough bytes were available
     *          from the ciphertext stream.
     * @throws IOException when an exception occurs while reading the ciphertextStream or when
     *         the header is too short.
     */
    private boolean tryReadHeader() throws IOException {
        if (endOfCiphertext) {
            throw new IOException("Ciphertext is too short");
        }
        readSomeCiphertext(header);
        if (header.remaining() > 0) {
            return false;
        } else {
            header.flip();
            try {
                decrypter.init(header, UNUSED_AAD);
                headerRead = true;
            } catch (GeneralSecurityException ex) {
                // TODO(b/74249330): Try to define the state of this.
                setUndefinedState();
                throw new IOException(ex);
            }
            return true;
        }
    }

    private void setUndefinedState() {
        definedState = false;
        plaintextSegment.limit(0);
    }

    /**
     * Tries to load the next plaintext segment.
     */
    private boolean tryLoadSegment() throws IOException {
        // Try filling the ciphertextSegment
        if (!endOfCiphertext) {
            readSomeCiphertext(ciphertextSegment);
        }
        if (ciphertextSegment.remaining() > 0 && !endOfCiphertext) {
            // we have not enough ciphertext for the next segment
            return false;
        }
        byte lastByte = 0;
        if (!endOfCiphertext) {
            lastByte = ciphertextSegment.get(ciphertextSegment.position() - 1);
            ciphertextSegment.position(ciphertextSegment.position() - 1);
        }
        ciphertextSegment.flip();
        plaintextSegment.clear();
        try {
            decrypter.decryptSegment(
                    ciphertextSegment, segmentNr, endOfCiphertext, plaintextSegment);
        } catch (GeneralSecurityException ex) {
            // The current segment did not validate.
            // Currently this means that decryption cannot resume.
            setUndefinedState();
            throw new IOException(ex.getMessage() + "\n" + toString()
                    + "\nsegmentNr:" + segmentNr
                    + " endOfCiphertext:" + endOfCiphertext,
                    ex);
        }
        segmentNr += 1;
        plaintextSegment.flip();
        ciphertextSegment.clear();
        if (!endOfCiphertext) {
            ciphertextSegment.clear();
            ciphertextSegment.limit(ciphertextSegmentSize + 1);
            ciphertextSegment.put(lastByte);
        }
        return true;
    }

    @Override
    public synchronized int read(ByteBuffer dst) throws IOException {
        if (!definedState) {
            throw new IOException("This StreamingAeadDecryptingChannel is in an undefined state");
        }
        if (!headerRead) {
            if (!tryReadHeader()) {
                return 0;
            }
            ciphertextSegment.clear();
            ciphertextSegment.limit(firstCiphertextSegmentSize + 1);
        }
        if (endOfPlaintext) {
            return -1;
        }
        int startPosition = dst.position();
        while (dst.remaining() > 0) {
            if (plaintextSegment.remaining() == 0) {
                if (endOfCiphertext) {
                    endOfPlaintext = true;
                    break;
                }
                if (!tryLoadSegment()) {
                    break;
                }
            }
            if (plaintextSegment.remaining() <= dst.remaining()) {
                dst.put(plaintextSegment);
            } else {
                int sliceSize = dst.remaining();
                ByteBuffer slice = plaintextSegment.duplicate();
                slice.limit(slice.position() + sliceSize);
                dst.put(slice);
                plaintextSegment.position(plaintextSegment.position() + sliceSize);
            }
        }
        int bytesRead = dst.position() - startPosition;
        if (bytesRead == 0 && endOfPlaintext) {
            return -1;
        } else {
            return bytesRead;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (ciphertextChannel != null) {
            ciphertextChannel.close();
            ciphertextChannel = null;
        }
    }

    @Override
    public synchronized boolean isOpen() {
        return ciphertextChannel.isOpen();
    }


    /* Returns the state of the channel. */
    @Override
    public synchronized String toString() {
        StringBuilder res = new StringBuilder();
        res.append("StreamingAeadDecryptingChannel")
                .append("\n\tsegmentNr:").append(segmentNr)
                .append("\tciphertextSegmentSize:").append(ciphertextSegmentSize)
                .append("\n\theaderRead:").append(headerRead)
                .append("\tendOfCiphertext:").append(endOfCiphertext)
                .append("\tendOfPlaintext:").append(endOfPlaintext)
                .append("\tdefinedState:").append(definedState)
                .append("\n\tHeader position:").append(header.position())
                .append("\tlimit:").append(header.position())
                .append("\n\tciphertextSegment position:").append(ciphertextSegment.position())
                .append("\tlimit:").append(ciphertextSegment.limit())
                .append("\n\tplaintextSegment position:").append(plaintextSegment.position())
                .append("\tlimit:").append(plaintextSegment.limit());
        return res.toString();
    }
}