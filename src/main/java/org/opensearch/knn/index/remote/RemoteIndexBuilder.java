/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.CheckedTriFunction;
import org.opensearch.common.StopWatch;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static org.opensearch.knn.index.codec.util.KNNCodecUtil.initializeVectorValues;

//TODO: Probably doesn't need to be a AbstractLifecycleComponent
@Log4j2
public class RemoteIndexBuilder extends AbstractLifecycleComponent {

    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    static final String VECTOR_REPO_NAME = "vector-repo"; // TODO: Get this from cluster setting
    public static final String VECTOR_PATH = "vectors";
    static final String VECTOR_BLOB_FILE_EXTENSION = ".knnvec";
    static final String DOC_ID_FILE_EXTENSION = ".knndid";
    static final String GRAPH_FILE_EXTENSION = ".knngraph";

    public RemoteIndexBuilder(Supplier<RepositoriesService> repositoriesServiceSupplier) {
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
    }

    /**
     * Gets the KNN repository container from the repository service which we can read/write to
     */
    public BlobStoreRepository getRepository() {
        RepositoriesService repositoriesService = repositoriesServiceSupplier.get();
        final Repository repository = repositoriesService.repository(VECTOR_REPO_NAME);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        return (BlobStoreRepository) repository;
    }

    /**
     * 1. upload files, 2 trigger build, 3 download the graph, 4 write to indexoutput
     *
     * @param fieldInfo
     * @param knnVectorValuesSupplier
     * @param totalLiveDocs
     */
    public void buildIndexRemotely(
        FieldInfo fieldInfo,
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
        int totalLiveDocs,
        SegmentWriteState segmentWriteState
    ) {
        try {
            StopWatch stopWatch = new StopWatch().start();
            writeFiles(fieldInfo, knnVectorValuesSupplier, totalLiveDocs, segmentWriteState);
            long time_in_millis = stopWatch.stop().totalTime().millis();
            log.info("Not triggering GPU build");
            log.info("Remote build took {} ms for vector field [{}]", time_in_millis, fieldInfo.getName());
        } catch (Exception e) {
            log.info("Remote Build Exception", e);
            e.printStackTrace();
        }
    }

    private void writeFiles(
        FieldInfo fieldInfo,
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
        int totalLiveDocs,
        SegmentWriteState segmentWriteState
    ) throws IOException, InterruptedException {
        log.info("Writing Files To Repository");
        // TODO: Only uploading vectors for now for benchmarking purposes
        BlobContainer blobContainer = getRepository().blobStore()
            .blobContainer(getRepository().basePath().add(RemoteIndexBuilder.VECTOR_PATH));

        if (blobContainer instanceof AsyncMultiStreamBlobContainer && KNNSettings.getParallelVectorUpload()) {
            log.info("Parallel Uploads Supported");
            KNNVectorValues<?> knnVectorValues = knnVectorValuesSupplier.get();
            initializeVectorValues(knnVectorValues);

            String blobName = fieldInfo.getName() + "_" + segmentWriteState.segmentInfo.name + VECTOR_BLOB_FILE_EXTENSION;
            WriteContext writeContext = new WriteContext.Builder().fileName(blobName).streamContextSupplier((partSize) -> {
                long contentLength = (long) knnVectorValues.bytesPerVector() * totalLiveDocs;
                long lastPartSize = (contentLength % partSize) != 0 ? contentLength % partSize : partSize;
                int numberOfParts = (int) ((contentLength % partSize) == 0 ? contentLength / partSize : (contentLength / partSize) + 1);

                log.info(
                    "Performing parallel upload with partSize: {}, numberOfParts: {}, lastPartSize: {}",
                    partSize,
                    numberOfParts,
                    lastPartSize
                );
                return new StreamContext(getTransferPartStreamSupplier(knnVectorValuesSupplier), partSize, lastPartSize, numberOfParts);
            })
                .fileSize((long) knnVectorValues.bytesPerVector() * totalLiveDocs)
                .failIfAlreadyExists(false)
                .writePriority(WritePriority.NORMAL)
                .uploadFinalizer((bool) -> {})
                .doRemoteDataIntegrityCheck(false)
                .expectedChecksum(null)
                .metadata(null)
                .build();

            final CountDownLatch latch = new CountDownLatch(1);
            ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(
                writeContext,
                new LatchedActionListener<>(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        log.info("Parallel Upload Completed");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.info("Parallel Upload Failed");
                        e.printStackTrace();
                    }
                }, latch)
            );
            latch.await();

            // Write Doc IDs
            InputStream docIdStream = new DocIdInputStream(knnVectorValuesSupplier.get());
            blobName = fieldInfo.getName() + "_" + segmentWriteState.segmentInfo.name + DOC_ID_FILE_EXTENSION;
            blobContainer.writeBlob(blobName, docIdStream, 4L * totalLiveDocs, false);
        } else {
            // Write Vectors
            InputStream vectorStream = new VectorValuesInputStream(knnVectorValuesSupplier.get());
            KNNVectorValues<?> knnVectorValues = knnVectorValuesSupplier.get();
            initializeVectorValues(knnVectorValues);
            String blobName = fieldInfo.getName() + "_" + segmentWriteState.segmentInfo.name + VECTOR_BLOB_FILE_EXTENSION;
            blobContainer.writeBlob(blobName, vectorStream, (long) knnVectorValues.bytesPerVector() * totalLiveDocs, false);
        }
    }

    private CheckedTriFunction<Integer, Long, Long, InputStreamContainer, IOException> getTransferPartStreamSupplier(
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier
    ) {
        // partNo: part number
        // size: partSize, may be different for last part
        // position: byte offset in the "file" for the part number
        return ((partNo, size, position) -> {
            log.info("Using InputStream with partNo: {}, size: {}, position: {}", partNo, size, position);
            VectorValuesInputStream vectorValuesInputStream = new VectorValuesInputStream(knnVectorValuesSupplier.get());
            long bytesSkipped = vectorValuesInputStream.skip(position);
            if (bytesSkipped != position) {
                throw new IllegalArgumentException("Skipped " + bytesSkipped + " bytes, expected to skip " + position);
            }
            vectorValuesInputStream.setBytesRemaining(size);
            return new InputStreamContainer(vectorValuesInputStream, size, position);
        });
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }
}
