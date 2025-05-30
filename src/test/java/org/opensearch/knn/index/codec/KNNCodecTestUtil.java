/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.Builder;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import java.util.Set;

import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.query.KNNQueryResult;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.store.IndexInputWithBuffer;
import org.opensearch.knn.jni.JNIService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.opensearch.knn.common.KNNConstants.INDEX_DESCRIPTION_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.SPACE_TYPE;
import static org.opensearch.knn.common.KNNConstants.VECTOR_DATA_TYPE_FIELD;
import static org.opensearch.test.OpenSearchTestCase.randomByteArrayOfLength;

public class KNNCodecTestUtil {

    // Utility class to help build FieldInfo
    public static class FieldInfoBuilder {
        private final String fieldName;
        private int fieldNumber;
        private boolean storeTermVector;
        private boolean omitNorms;
        private boolean storePayloads;
        private IndexOptions indexOptions;
        private DocValuesType docValuesType;
        private long dvGen;
        private final Map<String, String> attributes;
        private int pointDimensionCount;
        private int pointIndexDimensionCount;
        private int pointNumBytes;
        private int vectorDimension;
        private VectorSimilarityFunction vectorSimilarityFunction;
        private boolean softDeletes;
        private boolean isParentField;

        public static FieldInfoBuilder builder(String fieldName) {
            return new FieldInfoBuilder(fieldName);
        }

        private FieldInfoBuilder(String fieldName) {
            this.fieldName = fieldName;
            this.fieldNumber = 0;
            this.storeTermVector = false;
            this.omitNorms = true;
            this.storePayloads = true;
            this.indexOptions = IndexOptions.NONE;
            this.docValuesType = DocValuesType.BINARY;
            this.dvGen = 0;
            this.attributes = new HashMap<>();
            this.pointDimensionCount = 0;
            this.pointIndexDimensionCount = 0;
            this.pointNumBytes = 0;
            this.vectorDimension = 0;
            this.vectorSimilarityFunction = VectorSimilarityFunction.EUCLIDEAN;
            this.softDeletes = false;
            this.isParentField = false;
        }

        public FieldInfoBuilder fieldNumber(int fieldNumber) {
            this.fieldNumber = fieldNumber;
            return this;
        }

        public FieldInfoBuilder storeTermVector(boolean storeTermVector) {
            this.storeTermVector = storeTermVector;
            return this;
        }

        public FieldInfoBuilder omitNorms(boolean omitNorms) {
            this.omitNorms = omitNorms;
            return this;
        }

        public FieldInfoBuilder storePayloads(boolean storePayloads) {
            this.storePayloads = storePayloads;
            return this;
        }

        public FieldInfoBuilder indexOptions(IndexOptions indexOptions) {
            this.indexOptions = indexOptions;
            return this;
        }

        public FieldInfoBuilder docValuesType(DocValuesType docValuesType) {
            this.docValuesType = docValuesType;
            return this;
        }

        public FieldInfoBuilder dvGen(long dvGen) {
            this.dvGen = dvGen;
            return this;
        }

        public FieldInfoBuilder addAttribute(String key, String value) {
            this.attributes.put(key, value);
            return this;
        }

        public FieldInfoBuilder pointDimensionCount(int pointDimensionCount) {
            this.pointDimensionCount = pointDimensionCount;
            return this;
        }

        public FieldInfoBuilder pointIndexDimensionCount(int pointIndexDimensionCount) {
            this.pointIndexDimensionCount = pointIndexDimensionCount;
            return this;
        }

        public FieldInfoBuilder pointNumBytes(int pointNumBytes) {
            this.pointNumBytes = pointNumBytes;
            return this;
        }

        public FieldInfoBuilder vectorDimension(int vectorDimension) {
            this.vectorDimension = vectorDimension;
            return this;
        }

        public FieldInfoBuilder vectorSimilarityFunction(VectorSimilarityFunction vectorSimilarityFunction) {
            this.vectorSimilarityFunction = vectorSimilarityFunction;
            return this;
        }

        public FieldInfoBuilder softDeletes(boolean softDeletes) {
            this.softDeletes = softDeletes;
            return this;
        }

        public FieldInfoBuilder isParentField(boolean isParentField) {
            this.isParentField = isParentField;
            return this;
        }

        public FieldInfo build() {
            return new FieldInfo(
                fieldName,
                fieldNumber,
                storeTermVector,
                omitNorms,
                storePayloads,
                indexOptions,
                docValuesType,
                DocValuesSkipIndexType.NONE,

                dvGen,
                attributes,
                pointDimensionCount,
                pointIndexDimensionCount,
                pointNumBytes,
                vectorDimension,
                VectorEncoding.FLOAT32,
                vectorSimilarityFunction,
                softDeletes,
                isParentField
            );
        }
    }

    public static void assertFileInCorrectLocation(SegmentWriteState state, String expectedFile) throws IOException {
        assertTrue(Set.of(state.directory.listAll()).contains(expectedFile));
    }

    public static void assertValidFooter(Directory dir, String filename) throws IOException {
        ChecksumIndexInput indexInput = dir.openChecksumInput(filename);
        indexInput.seek(indexInput.length() - CodecUtil.footerLength());
        CodecUtil.checkFooter(indexInput);
        indexInput.close();
    }

    public static void assertLoadableByEngine(
        Map<String, ?> methodParameters,
        SegmentWriteState state,
        String fileName,
        KNNEngine knnEngine,
        SpaceType spaceType,
        int dimension
    ) {
        try (final IndexInput indexInput = state.directory.openInput(fileName, IOContext.DEFAULT)) {
            final IndexInputWithBuffer indexInputWithBuffer = new IndexInputWithBuffer(indexInput);
            long indexPtr = JNIService.loadIndex(
                indexInputWithBuffer,
                Maps.newHashMap(ImmutableMap.of(SPACE_TYPE, spaceType.getValue())),
                knnEngine
            );
            int k = 2;
            float[] queryVector = new float[dimension];
            KNNQueryResult[] results = JNIService.queryIndex(indexPtr, queryVector, k, methodParameters, knnEngine, null, 0, null);
            assertTrue(results.length > 0);
            JNIService.free(indexPtr, knnEngine);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertBinaryIndexLoadableByEngine(
        SegmentWriteState state,
        String fileName,
        KNNEngine knnEngine,
        SpaceType spaceType,
        int dimension,
        VectorDataType vectorDataType
    ) {
        try (final IndexInput indexInput = state.directory.openInput(fileName, IOContext.DEFAULT)) {
            final IndexInputWithBuffer indexInputWithBuffer = new IndexInputWithBuffer(indexInput);
            long indexPtr = JNIService.loadIndex(
                indexInputWithBuffer,
                Maps.newHashMap(
                    ImmutableMap.of(
                        SPACE_TYPE,
                        spaceType.getValue(),
                        INDEX_DESCRIPTION_PARAMETER,
                        "BHNSW32",
                        VECTOR_DATA_TYPE_FIELD,
                        vectorDataType.getValue()
                    )
                ),
                knnEngine
            );
            int k = 2;
            byte[] queryVector = new byte[dimension];
            KNNQueryResult[] results = JNIService.queryBinaryIndex(indexPtr, queryVector, k, null, knnEngine, null, 0, null);
            assertTrue(results.length > 0);
            JNIService.free(indexPtr, knnEngine);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Builder(builderMethodName = "segmentInfoBuilder")
    public static SegmentInfo newSegmentInfo(final Directory directory, final String segmentName, int docsInSegment, final Codec codec) {
        return new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            segmentName,
            docsInSegment,
            false,
            false,
            codec,
            Collections.emptyMap(),
            randomByteArrayOfLength(StringHelper.ID_LENGTH),
            ImmutableMap.of(),
            Sort.INDEXORDER
        );
    }
}
