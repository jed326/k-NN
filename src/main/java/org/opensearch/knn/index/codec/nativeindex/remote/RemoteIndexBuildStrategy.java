/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.nativeindex.remote;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.NotImplementedException;
import org.opensearch.common.StopWatch;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.codec.nativeindex.NativeIndexBuildStrategy;
import org.opensearch.knn.index.codec.nativeindex.model.BuildIndexParams;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.util.function.Supplier;

import static org.opensearch.knn.index.KNNSettings.KNN_INDEX_REMOTE_VECTOR_BUILD_SETTING;
import static org.opensearch.knn.index.KNNSettings.KNN_INDEX_REMOTE_VECTOR_BUILD_THRESHOLD_SETTING;
import static org.opensearch.knn.index.KNNSettings.KNN_REMOTE_VECTOR_REPO_SETTING;
import static org.opensearch.knn.index.codec.util.KNNCodecUtil.initializeVectorValues;
import static org.opensearch.knn.plugin.stats.KNNRemoteIndexBuildValue.BUILD_REQUEST_FAILURE_COUNT;
import static org.opensearch.knn.plugin.stats.KNNRemoteIndexBuildValue.BUILD_REQUEST_SUCCESS_COUNT;
import static org.opensearch.knn.plugin.stats.KNNRemoteIndexBuildValue.READ_FAILURE_COUNT;
import static org.opensearch.knn.plugin.stats.KNNRemoteIndexBuildValue.READ_SUCCESS_COUNT;
import static org.opensearch.knn.plugin.stats.KNNRemoteIndexBuildValue.READ_TIME;
import static org.opensearch.knn.plugin.stats.KNNRemoteIndexBuildValue.REMOTE_INDEX_BUILD_CURRENT_OPERATIONS;
import static org.opensearch.knn.plugin.stats.KNNRemoteIndexBuildValue.REMOTE_INDEX_BUILD_CURRENT_SIZE;
import static org.opensearch.knn.plugin.stats.KNNRemoteIndexBuildValue.REMOTE_INDEX_BUILD_TIME;
import static org.opensearch.knn.plugin.stats.KNNRemoteIndexBuildValue.WAITING_TIME;
import static org.opensearch.knn.plugin.stats.KNNRemoteIndexBuildValue.WRITE_FAILURE_COUNT;
import static org.opensearch.knn.plugin.stats.KNNRemoteIndexBuildValue.WRITE_SUCCESS_COUNT;
import static org.opensearch.knn.plugin.stats.KNNRemoteIndexBuildValue.WRITE_TIME;

/**
 * This class orchestrates building vector indices. It handles uploading data to a repository, submitting a remote
 * build request, awaiting upon the build request to complete, and finally downloading the data from a repository.
 */
@Log4j2
@ExperimentalApi
public class RemoteIndexBuildStrategy implements NativeIndexBuildStrategy {

    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final NativeIndexBuildStrategy fallbackStrategy;
    private final IndexSettings indexSettings;

    static final String VECTOR_BLOB_FILE_EXTENSION = ".knnvec";
    static final String DOC_ID_FILE_EXTENSION = ".knndid";
    static final String VECTORS_PATH = "_vectors";

    /**
     * Public constructor, intended to be called by {@link org.opensearch.knn.index.codec.nativeindex.NativeIndexBuildStrategyFactory} based in
     * part on the return value from {@link RemoteIndexBuildStrategy#shouldBuildIndexRemotely}
     * @param repositoriesServiceSupplier       A supplier for {@link RepositoriesService} used to interact with a repository
     * @param fallbackStrategy                  Delegate {@link NativeIndexBuildStrategy} used to fall back to local build
     * @param indexSettings                    {@link IndexSettings} used to retrieve information about the index
     */
    public RemoteIndexBuildStrategy(
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        NativeIndexBuildStrategy fallbackStrategy,
        IndexSettings indexSettings
    ) {
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.fallbackStrategy = fallbackStrategy;
        this.indexSettings = indexSettings;
    }

    /**
     * @param indexSettings         {@link IndexSettings} used to check if index setting is enabled for the feature
     * @param vectorBlobLength      The size of the vector blob, used to determine if the size threshold is met
     * @return true if remote index build should be used, else false
     */
    public static boolean shouldBuildIndexRemotely(IndexSettings indexSettings, long vectorBlobLength) {
        if (indexSettings == null) {
            return false;
        }

        // If setting is not enabled, return false
        if (!indexSettings.getValue(KNN_INDEX_REMOTE_VECTOR_BUILD_SETTING)) {
            log.debug("Remote index build is disabled for index: [{}]", indexSettings.getIndex().getName());
            return false;
        }

        // If vector repo is not configured, return false
        String vectorRepo = KNNSettings.state().getSettingValue(KNN_REMOTE_VECTOR_REPO_SETTING.getKey());
        if (vectorRepo == null || vectorRepo.isEmpty()) {
            log.debug("Vector repo is not configured, falling back to local build for index: [{}]", indexSettings.getIndex().getName());
            return false;
        }

        // If size threshold is not met, return false
        if (vectorBlobLength < indexSettings.getValue(KNN_INDEX_REMOTE_VECTOR_BUILD_THRESHOLD_SETTING).getBytes()) {
            log.debug(
                "Data size [{}] is less than remote index build threshold [{}], falling back to local build for index [{}]",
                vectorBlobLength,
                indexSettings.getValue(KNN_INDEX_REMOTE_VECTOR_BUILD_THRESHOLD_SETTING).getBytes(),
                indexSettings.getIndex().getName()
            );
            return false;
        }

        return true;
    }

    /**
     * Entry point for flush/merge operations. This method orchestrates the following:
     *      1. Writes required data to repository
     *      2. Triggers index build
     *      3. Awaits on vector build to complete
     *      4. Downloads index file and writes to indexOutput
     *
     * @param indexInfo
     * @throws IOException
     */
    @Override
    public void buildAndWriteIndex(BuildIndexParams indexInfo) throws IOException {
        StopWatch stopWatch;
        long time_in_millis;
        final VectorRepositoryAccessor vectorRepositoryAccessor;

        StopWatch remoteBuildTimeStopwatch = new StopWatch();
        KNNVectorValues<?> knnVectorValues = indexInfo.getKnnVectorValuesSupplier().get();
        initializeVectorValues(knnVectorValues);
        startRemoteIndexBuildStats((long) indexInfo.getTotalLiveDocs() * knnVectorValues.bytesPerVector(), remoteBuildTimeStopwatch);

        // 1. Write required data to repository
        stopWatch = new StopWatch().start();
        try {
            vectorRepositoryAccessor = new DefaultVectorRepositoryAccessor(getRepository(), indexSettings);
            // We create a new time based UUID per file in order to avoid conflicts across shards. It is also very difficult to get the
            // shard id in this context.
            String blobName = UUIDs.base64UUID() + "_" + indexInfo.getFieldName() + "_" + indexInfo.getSegmentWriteState().segmentInfo.name;
            vectorRepositoryAccessor.writeToRepository(
                blobName,
                indexInfo.getTotalLiveDocs(),
                indexInfo.getVectorDataType(),
                indexInfo.getKnnVectorValuesSupplier()
            );
            time_in_millis = stopWatch.stop().totalTime().millis();
            WRITE_SUCCESS_COUNT.increment();
            WRITE_TIME.incrementBy(time_in_millis);
            log.debug("Repository write took {} ms for vector field [{}]", time_in_millis, indexInfo.getFieldName());
        } catch (Exception e) {
            time_in_millis = stopWatch.stop().totalTime().millis();
            WRITE_FAILURE_COUNT.increment();
            log.error("Repository write failed after {} ms for vector field [{}]", time_in_millis, indexInfo.getFieldName(), e);
            handleFailure(indexInfo, knnVectorValues.bytesPerVector(), remoteBuildTimeStopwatch);
            return;
        }

        // 2. Triggers index build
        stopWatch = new StopWatch().start();
        try {
            submitVectorBuild();
            time_in_millis = stopWatch.stop().totalTime().millis();
            BUILD_REQUEST_SUCCESS_COUNT.increment();
            log.debug("Submit vector build took {} ms for vector field [{}]", time_in_millis, indexInfo.getFieldName());
        } catch (Exception e) {
            BUILD_REQUEST_FAILURE_COUNT.increment();
            log.error("Submit vector failed after {} ms for vector field [{}]", time_in_millis, indexInfo.getFieldName(), e);
            handleFailure(indexInfo, knnVectorValues.bytesPerVector(), remoteBuildTimeStopwatch);
            return;
        }

        // 3. Awaits on vector build to complete
        stopWatch = new StopWatch().start();
        try {
            awaitVectorBuild();
            time_in_millis = stopWatch.stop().totalTime().millis();
            WAITING_TIME.incrementBy(time_in_millis);
            log.debug("Await vector build took {} ms for vector field [{}]", time_in_millis, indexInfo.getFieldName());
        } catch (Exception e) {
            log.debug("Await vector build failed after {} ms for vector field [{}]", time_in_millis, indexInfo.getFieldName());
            handleFailure(indexInfo, knnVectorValues.bytesPerVector(), remoteBuildTimeStopwatch);
            return;
        }

        // 4. Downloads index file and writes to indexOutput
        stopWatch = new StopWatch().start();
        try {
            assert vectorRepositoryAccessor != null;
            vectorRepositoryAccessor.readFromRepository();
            time_in_millis = stopWatch.stop().totalTime().millis();
            READ_SUCCESS_COUNT.increment();
            READ_TIME.incrementBy(time_in_millis);
            log.debug("Repository read took {} ms for vector field [{}]", time_in_millis, indexInfo.getFieldName());
        } catch (Exception e) {
            time_in_millis = stopWatch.stop().totalTime().millis();
            READ_FAILURE_COUNT.increment();
            log.error("Repository read failed after {} ms for vector field [{}]", time_in_millis, indexInfo.getFieldName(), e);
            handleFailure(indexInfo, knnVectorValues.bytesPerVector(), remoteBuildTimeStopwatch);
            return;
        }

        endRemoteIndexBuildStats((long) indexInfo.getTotalLiveDocs() * knnVectorValues.bytesPerVector(), stopWatch);
    }

    /**
     * Gets the KNN repository container from the repository service.
     *
     * @return {@link RepositoriesService}
     * @throws RepositoryMissingException if repository is not registered or if {@link KNN_REMOTE_VECTOR_REPO_SETTING} is not set
     */
    private BlobStoreRepository getRepository() throws RepositoryMissingException {
        RepositoriesService repositoriesService = repositoriesServiceSupplier.get();
        assert repositoriesService != null;
        String vectorRepo = KNNSettings.state().getSettingValue(KNN_REMOTE_VECTOR_REPO_SETTING.getKey());
        if (vectorRepo == null || vectorRepo.isEmpty()) {
            throw new RepositoryMissingException("Vector repository " + KNN_REMOTE_VECTOR_REPO_SETTING.getKey() + " is not registered");
        }
        final Repository repository = repositoriesService.repository(vectorRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        return (BlobStoreRepository) repository;
    }

    /**
     * Submit vector build request to remote vector build service
     *
     */
    private void submitVectorBuild() {
        throw new NotImplementedException();
    }

    /**
     * Wait on remote vector build to complete
     */
    private void awaitVectorBuild() {
        throw new NotImplementedException();
    }

    private void startRemoteIndexBuildStats(long size, StopWatch stopWatch) {
        stopWatch.start();
        REMOTE_INDEX_BUILD_CURRENT_OPERATIONS.increment();
        REMOTE_INDEX_BUILD_CURRENT_SIZE.incrementBy(size);
    }

    private void endRemoteIndexBuildStats(long size, StopWatch stopWatch) {
        long time_in_millis = stopWatch.stop().totalTime().millis();
        REMOTE_INDEX_BUILD_CURRENT_OPERATIONS.decrement();
        REMOTE_INDEX_BUILD_CURRENT_SIZE.decrementBy(size);
        REMOTE_INDEX_BUILD_TIME.incrementBy(time_in_millis);
    }

    /**
     * Helper method to collect remote index build metrics on failure and invoke fallback strategy
     * @param indexParams
     * @param bytesPerVector
     * @throws IOException
     */
    private void handleFailure(BuildIndexParams indexParams, long bytesPerVector, StopWatch stopWatch) throws IOException {
        endRemoteIndexBuildStats(indexParams.getTotalLiveDocs() * bytesPerVector, stopWatch);
        fallbackStrategy.buildAndWriteIndex(indexParams);
    }
}
