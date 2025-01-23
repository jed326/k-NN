/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.io.InputStream;

@Log4j2
public class TestInputStream extends InputStream {
    private final IndexInput indexInput;
    private long position = 0;
    private final long size;

    public TestInputStream(IndexInput indexInput) {
        this.indexInput = indexInput.clone();
        this.size = indexInput.length();
    }

    @Override
    public int read() throws IOException {
        IndexInput tmp = indexInput.clone();
        if (position > size) {
            tmp.close();
            return -1;
        } else {
            tmp.seek(position);
            position = tmp.getFilePointer();
            tmp.close();
            return tmp.readByte() & 0xFF;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        IndexInput tmp = indexInput.clone();
        int bytesToRead = 0;
        if (position >= size) {
            return -1;
        } else if (position + len > size) {
            bytesToRead = (int) (size - position);
        } else {
            bytesToRead = len;
        }
        position += len;
        tmp.readBytes(b, off, bytesToRead);
        tmp.close();
        return bytesToRead;
    }
}
