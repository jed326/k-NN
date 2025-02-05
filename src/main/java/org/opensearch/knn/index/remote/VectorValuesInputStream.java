/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.knn.index.vectorvalues.KNNFloatVectorValues;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.opensearch.knn.index.codec.util.KNNCodecUtil.initializeVectorValues;

@Log4j2
public class VectorValuesInputStream extends InputStream {

    static final int five_mb = 5 * 1024 * 1024; // 1MB = 1024KB, 1KB = 1024 bytes

    private final KNNVectorValues<?> knnVectorValues;
    private ByteBuffer currentBuffer;
    private int bufferPosition = 0;

    // TODO: currently skip eats up all the bytes remaining, so we expose a setting to set it after skip.
    @Setter
    private long bytesRemaining; // Number of bytes this stream is allowed to read

    public VectorValuesInputStream(KNNVectorValues<?> knnVectorValues, long readLimit) throws IOException {
        this.bytesRemaining = readLimit;
        this.knnVectorValues = knnVectorValues;
        initializeVectorValues(knnVectorValues);
        // check no more docs case
        int bytesPerVector = knnVectorValues.bytesPerVector();
        // TODO: For S3 the retryable input stream is backed by a buffer of part size, so all readLimit bytes are loaded onto heap at once (16mb chunks by default)
        // I don't think we actually need to do a buffered approach in this InputStream then as this is basically just creating an intermediary buffer
        currentBuffer = ByteBuffer.allocate(bytesPerVector).order(ByteOrder.LITTLE_ENDIAN);
    }

    public VectorValuesInputStream(KNNVectorValues<?> knnVectorValues) throws IOException {
        this(knnVectorValues, Long.MAX_VALUE);
    }

    @Override
    public int read() throws IOException {
        if (bytesRemaining <= 0) {
            return -1;
        }

        if (currentBuffer == null || bufferPosition >= currentBuffer.capacity()) {
            reloadBuffer();
            if (currentBuffer == null) {
                return -1;
            }
        }
        bytesRemaining--;
        return currentBuffer.get(bufferPosition++) & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (bytesRemaining <= 0) {
            return -1;
        }

        if (currentBuffer == null) {
            return -1;
        }

        int available = currentBuffer.capacity() - bufferPosition;
        if (available <= 0) {
            reloadBuffer();
            if (currentBuffer == null) {
                return -1;
            }
            available = currentBuffer.capacity() - bufferPosition;
        }
        available = (int) Math.min(available, bytesRemaining);
        int bytesToRead = Math.min(available, len);
        currentBuffer.position(bufferPosition);
        currentBuffer.get(b, off, bytesToRead);
        bufferPosition += bytesToRead;
        bytesRemaining -= bytesToRead;
        return bytesToRead;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void reset() throws IOException {
        // TODO: Implement
    }

    @Override
    public void mark(int readlimit) {
        // TODO: Implement
    }

    // Buffer is full so clear it and add the next vector(s) to the buffer
    private void reloadBuffer() throws IOException {
        int docId = knnVectorValues.nextDoc();
        if (docId != -1 && docId != DocIdSetIterator.NO_MORE_DOCS) {
            // TODO: Assume float for now, need to make generic
            float[] vector = ((KNNFloatVectorValues) knnVectorValues).getVector();
            // this is a costly operation. We should optimize this
            currentBuffer.clear();
            currentBuffer.asFloatBuffer().put(vector);
            bufferPosition = 0;
        } else {
            currentBuffer = null;

        }
    }
}
