/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import org.opensearch.knn.index.engine.qframe.QuantizationConfig;
import org.opensearch.knn.memoryoptsearch.VectorSearcher;
import org.opensearch.knn.quantization.enums.ScalarQuantizationType;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.knn.common.KNNConstants.ENCODER_BINARY;
import static org.opensearch.knn.common.KNNConstants.ENCODER_FLAT;
import static org.opensearch.knn.common.KNNConstants.ENCODER_SQ;
import static org.opensearch.knn.common.KNNConstants.METHOD_ENCODER_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.METHOD_HNSW;

/**
 * This class encapsulates a determination logic for memory optimized search.
 * Memory-optimized-search may not be applied to a certain type of index even {@link KNNEngine} returns a non-null
 * {@link org.opensearch.knn.memoryoptsearch.VectorSearcherFactory}.
 * The overall logic will be made based on the given method context and quantization configuration.
 */
public class MemoryOptimizedSearchSupportSpec {
    private static final Set<String> SUPPORTED_HNSW_ENCODING = Set.of(ENCODER_FLAT, ENCODER_SQ, ENCODER_BINARY);

    /**
     * Determine whether if a KNN field supports memory-optimized-search.
     * If it is supported, then the field can perform memory-optimized search via {@link VectorSearcher}.
     * Which can be obtained from a factory acquired from {@link KNNEngine#getVectorSearcherFactory()}.
     *
     * @param methodContextOpt   Optional method context.
     * @param quantizationConfig Quantization configuration.
     * @param modelId Model id.
     * @return True if memory-optimized-search is supported, otherwise false.
     */
    public static boolean supported(
        final Optional<KNNMethodContext> methodContextOpt,
        final QuantizationConfig quantizationConfig,
        final Optional<String> modelId
    ) {
        // PQ is not supported.
        if (modelId.isPresent()) {
            return false;
        }

        if (methodContextOpt.isPresent()) {
            final KNNMethodContext methodContext = methodContextOpt.get();
            final KNNEngine engine = methodContext.getKnnEngine();

            // We support Lucene engine
            if (engine == KNNEngine.LUCENE) {
                return true;
            }

            // We don't support non-FAISS engine
            if (engine != KNNEngine.FAISS) {
                return false;
            }

            // We only support HNSW method.
            final MethodComponentContext methodComponentContext = methodContext.getMethodComponentContext();
            if (methodComponentContext.getName().equals(METHOD_HNSW) == false) {
                return false;
            }

            // Check whether it is a supported quantization.
            if (quantizationConfig != null && quantizationConfig.getQuantizationType() != null) {
                if (isSupportedQuantization(quantizationConfig) == false) {
                    return false;
                }
            }

            // We only support Flat and SQ encoder for HNSW.
            final Map<String, Object> parameters = methodComponentContext.getParameters();
            final Object methodComponentContextObj = parameters.get(METHOD_ENCODER_PARAMETER);
            if ((methodComponentContextObj instanceof MethodComponentContext) == false) {
                return false;
            }

            // Check whether HNSW encoding is supported.
            if (SUPPORTED_HNSW_ENCODING.contains(((MethodComponentContext) methodComponentContextObj).getName()) == false) {
                return false;
            }

            return true;
        }

        return false;
    }

    private static boolean isSupportedQuantization(final QuantizationConfig quantizationConfig) {
        final ScalarQuantizationType quantizationType = quantizationConfig.getQuantizationType();
        return quantizationType == ScalarQuantizationType.ONE_BIT
            || quantizationType == ScalarQuantizationType.TWO_BIT
            || quantizationType == ScalarQuantizationType.FOUR_BIT;
    }
}
