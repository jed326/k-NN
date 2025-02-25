/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.nativeindex.remote;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.NotImplementedException;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.CheckedTriFunction;
import org.opensearch.common.StopWatch;
import org.opensearch.common.StreamContext;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexSettings;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.codec.nativeindex.NativeIndexBuildStrategy;
import org.opensearch.knn.index.codec.nativeindex.model.BuildIndexParams;
import org.opensearch.knn.index.store.IndexOutputWithBuffer;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static org.opensearch.knn.index.KNNSettings.KNN_INDEX_REMOTE_VECTOR_BUILD_SETTING;
import static org.opensearch.knn.index.KNNSettings.KNN_REMOTE_VECTOR_REPO_SETTING;
import static org.opensearch.knn.index.codec.util.KNNCodecUtil.initializeVectorValues;

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

    private static final String VECTOR_BLOB_FILE_EXTENSION = ".knnvec";
    private static final String DOC_ID_FILE_EXTENSION = ".knndid";
    private static final String VECTORS_PATH = "_vectors";

    /**
     * Public constructor
     *
     * @param repositoriesServiceSupplier   A supplier for {@link RepositoriesService} used for interacting with repository
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
     * @return whether to use the remote build feature
     */
    public static boolean shouldBuildIndexRemotely(IndexSettings indexSettings, long vectorBlobLength) {

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
        if (vectorBlobLength < KNNSettings.getKnnRemoteVectorBuildThreshold().getBytes()) {
            log.debug(
                "Data size [{}] is less than remote index build threshold [{}], falling back to local build for index [{}]",
                vectorBlobLength,
                KNNSettings.getKnnRemoteVectorBuildThreshold().getBytes(),
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
        try {
            stopWatch = new StopWatch().start();
            // We create a new time based UUID per file in order to avoid conflicts across shards. It is also very difficult to get the
            // shard id in this context.
            String blobName = UUIDs.base64UUID() + "_" + indexInfo.getFieldName() + "_" + indexInfo.getSegmentWriteState().segmentInfo.name;
            writeToRepository(
                blobName,
                indexInfo.getTotalLiveDocs(),
                indexInfo.getVectorDataType(),
                indexInfo.getKnnVectorValuesSupplier()
            );
            time_in_millis = stopWatch.stop().totalTime().millis();
            log.debug("Repository write took {} ms for vector field [{}]", time_in_millis, indexInfo.getFieldName());

            stopWatch = new StopWatch().start();
            submitVectorBuild();
            time_in_millis = stopWatch.stop().totalTime().millis();
            log.debug("Submit vector build took {} ms for vector field [{}]", time_in_millis, indexInfo.getFieldName());

            stopWatch = new StopWatch().start();
            BlobPath downloadPath = awaitVectorBuild();
            time_in_millis = stopWatch.stop().totalTime().millis();
            log.debug("Await vector build took {} ms for vector field [{}]", time_in_millis, indexInfo.getFieldName());

            stopWatch = new StopWatch().start();
            readFromRepository(downloadPath, indexInfo.getIndexOutputWithBuffer());
            time_in_millis = stopWatch.stop().totalTime().millis();
            log.debug("Repository read took {} ms for vector field [{}]", time_in_millis, indexInfo.getFieldName());
        } catch (Exception e) {
            // TODO: This needs more robust failure handling
            log.warn("Failed to build index remotely", e);
            fallbackStrategy.buildAndWriteIndex(indexInfo);
        }
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
     * This method is responsible for writing both the vector blobs and doc ids provided by {@param knnVectorValuesSupplier} to the vector repository configured by {@link KNN_REMOTE_VECTOR_REPO_SETTING}.
     * If the repository implements {@link AsyncMultiStreamBlobContainer}, then parallel uploads will be used. Parallel uploads are backed by a {@link WriteContext}, for which we have a custom
     * {@link org.opensearch.common.blobstore.stream.write.StreamContextSupplier} implementation.
     *
     * @see RemoteIndexBuildStrategy#getStreamContext
     * @see RemoteIndexBuildStrategy#getTransferPartStreamSupplier
     *
     * @param blobName                  Base name of the blobs we are writing, excluding file extensions
     * @param totalLiveDocs             Number of documents we are processing. This is used to compute the size of the blob we are writing
     * @param vectorDataType            Data type of the vector (FLOAT, BYTE, BINARY)
     * @param knnVectorValuesSupplier   Supplier for {@link KNNVectorValues}
     * @throws IOException
     * @throws InterruptedException
     */
    private void writeToRepository(
        String blobName,
        int totalLiveDocs,
        VectorDataType vectorDataType,
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier
    ) throws IOException, InterruptedException {
        // Get the blob container based on blobName and the repo base path. This is where the blobs will be written to.
        BlobPath path = getRepository().basePath().add(indexSettings.getUUID() + VECTORS_PATH);
        BlobContainer blobContainer = getRepository().blobStore().blobContainer(path);

        KNNVectorValues<?> knnVectorValues = knnVectorValuesSupplier.get();
        initializeVectorValues(knnVectorValues);
        long vectorBlobLength = (long) knnVectorValues.bytesPerVector() * totalLiveDocs;

        if (blobContainer instanceof AsyncMultiStreamBlobContainer) {
            // First initiate vectors upload
            log.debug("Repository {} Supports Parallel Blob Upload", getRepository());
            // WriteContext is the main entry point into asyncBlobUpload. It stores all of our upload configurations, analogous to
            // BuildIndexParams
            WriteContext writeContext = new WriteContext.Builder().fileName(blobName + VECTOR_BLOB_FILE_EXTENSION)
                .streamContextSupplier((partSize) -> getStreamContext(partSize, vectorBlobLength, knnVectorValuesSupplier, vectorDataType))
                .fileSize(vectorBlobLength)
                .failIfAlreadyExists(true)
                .writePriority(WritePriority.NORMAL)
                // TODO: Checksum implementations -- It is difficult to calculate a checksum on the knnVectorValues as
                // there is no underlying file upon which we can create the checksum. We should be able to create a
                // checksum still by iterating through once, however this will be an expensive operation.
                .uploadFinalizer((bool) -> {})
                .doRemoteDataIntegrityCheck(false)
                .expectedChecksum(null)
                .build();

            final CountDownLatch latch = new CountDownLatch(1);
            ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(
                writeContext,
                new LatchedActionListener<>(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        log.debug(
                            "Parallel upload succeeded for blob {} with size {}",
                            blobName + VECTOR_BLOB_FILE_EXTENSION,
                            vectorBlobLength
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.debug(
                            "Parallel upload failed for blob {} with size {}",
                            blobName + VECTOR_BLOB_FILE_EXTENSION,
                            vectorBlobLength,
                            e
                        );
                    }
                }, latch)
            );

            // Then upload doc id blob before waiting on vector uploads
            // TODO: We wrap with a BufferedInputStream to support retries. We can tune this buffer size to optimize performance.
            InputStream docStream = new BufferedInputStream(new DocIdInputStream(knnVectorValuesSupplier.get()));
            // Note: We do not use the parallel upload API here as the doc id blob will be much smaller than the vector blob
            log.debug(
                "Writing {} bytes for {} docs ids to {}",
                vectorBlobLength,
                totalLiveDocs * Integer.BYTES,
                blobName + DOC_ID_FILE_EXTENSION
            );
            blobContainer.writeBlob(blobName + DOC_ID_FILE_EXTENSION, docStream, (long) totalLiveDocs * Integer.BYTES, true);
            latch.await();
        } else {
            log.debug("Repository {} Does Not Support Parallel Blob Upload", getRepository());
            // Write Vectors
            InputStream vectorStream = new BufferedInputStream(new VectorValuesInputStream(knnVectorValuesSupplier.get(), vectorDataType));
            log.debug("Writing {} bytes for {} docs to {}", vectorBlobLength, totalLiveDocs, blobName + VECTOR_BLOB_FILE_EXTENSION);
            blobContainer.writeBlob(blobName + VECTOR_BLOB_FILE_EXTENSION, vectorStream, vectorBlobLength, true);

            // Then write doc ids
            InputStream docStream = new BufferedInputStream(new DocIdInputStream(knnVectorValuesSupplier.get()));
            log.debug(
                "Writing {} bytes for {} docs ids to {}",
                vectorBlobLength,
                totalLiveDocs * Integer.BYTES,
                blobName + DOC_ID_FILE_EXTENSION
            );
            blobContainer.writeBlob(blobName + DOC_ID_FILE_EXTENSION, docStream, (long) totalLiveDocs * Integer.BYTES, true);
        }
    }

    /**
     * Returns a {@link StreamContext}. Intended to be invoked as a {@link org.opensearch.common.blobstore.stream.write.StreamContextSupplier},
     * which takes the partSize determined by the repository implementation and calculates the number of parts as well as handles the last part of the stream.
     *
     * @see RemoteIndexBuildStrategy#getTransferPartStreamSupplier
     *
     * @param partSize                  Size of each InputStream to be uploaded in parallel. Provided by repository implementation
     * @param vectorBlobLength          Total size of the vectors across all InputStreams
     * @param knnVectorValuesSupplier   Supplier for {@link KNNVectorValues}
     * @param vectorDataType            Data type of the vector (FLOAT, BYTE, BINARY)
     * @return a {@link StreamContext} with a function that will create {@link InputStream}s of {@param partSize}
     */
    private StreamContext getStreamContext(
        long partSize,
        long vectorBlobLength,
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
        VectorDataType vectorDataType
    ) {
        long lastPartSize = (vectorBlobLength % partSize) != 0 ? vectorBlobLength % partSize : partSize;
        int numberOfParts = (int) ((vectorBlobLength % partSize) == 0 ? vectorBlobLength / partSize : (vectorBlobLength / partSize) + 1);
        return new StreamContext(
            getTransferPartStreamSupplier(knnVectorValuesSupplier, vectorDataType),
            partSize,
            lastPartSize,
            numberOfParts
        );
    }

    /**
     * This method handles creating {@link VectorValuesInputStream}s based on the part number, the requested size of the stream part, and the position that the stream starts at within the underlying {@link KNNVectorValues}
     *
     * @param knnVectorValuesSupplier       Supplier for {@link KNNVectorValues}
     * @param vectorDataType                Data type of the vector (FLOAT, BYTE, BINARY)
     * @return a function with which the repository implementation will use to create {@link VectorValuesInputStream}s of specific sizes and start positions.
     */
    private CheckedTriFunction<Integer, Long, Long, InputStreamContainer, IOException> getTransferPartStreamSupplier(
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
        VectorDataType vectorDataType
    ) {
        return ((partNo, size, position) -> {
            log.info("Creating InputStream for partNo: {}, size: {}, position: {}", partNo, size, position);
            VectorValuesInputStream vectorValuesInputStream = new VectorValuesInputStream(
                knnVectorValuesSupplier.get(),
                vectorDataType,
                position,
                size
            );
            return new InputStreamContainer(vectorValuesInputStream, size, position);
        });
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
     * @return BlobPath     The path from which we should perform download
     */
    private BlobPath awaitVectorBuild() throws NotImplementedException {
        throw new NotImplementedException();
    }

    /**
     * Read constructed vector file from remote repository and write to IndexOutput
     */
    @VisibleForTesting
    void readFromRepository(BlobPath downloadPath, IndexOutputWithBuffer indexOutputWithBuffer) throws IOException {
        BlobContainer blobContainer = getRepository().blobStore().blobContainer(downloadPath.parent());
        // TODO: We are using the sequential download API as multi-part parallel download is difficult for us to implement today and
        // requires some changes in core. For more details, see: https://github.com/opensearch-project/k-NN/issues/2464
        String fileName = downloadPath.toArray()[downloadPath.toArray().length - 1];
        InputStream graphStream = blobContainer.readBlob(fileName);
        indexOutputWithBuffer.writeFromStreamWithBuffer(graphStream);
    }
}
