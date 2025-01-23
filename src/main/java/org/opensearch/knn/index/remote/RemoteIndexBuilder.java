/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

import static org.opensearch.knn.index.codec.util.KNNCodecUtil.buildEngineFileName;

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
    ) throws IOException {
        log.info("Writing Files To Repository");
        writeFiles(fieldInfo, knnVectorValuesSupplier, totalLiveDocs, segmentWriteState);
        log.info("Triggering Build");
        triggerBuild(fieldInfo, segmentWriteState);
        log.info("Downloading Files");
        downloadFiles(fieldInfo, segmentWriteState);
    }

    private void writeFiles(
        FieldInfo fieldInfo,
        Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
        int totalLiveDocs,
        SegmentWriteState segmentWriteState
    ) throws IOException {
        BlobContainer blobContainer = getRepository().blobStore()
            .blobContainer(getRepository().basePath().add(RemoteIndexBuilder.VECTOR_PATH));

        // Write Vectors
        InputStream vectorStream = new VectorValuesInputStream(knnVectorValuesSupplier.get());
        KNNVectorValues<?> knnVectorValues = knnVectorValuesSupplier.get();
        knnVectorValues.nextDoc();
        knnVectorValues.getVector();
        String blobName = fieldInfo.getName() + "_" + segmentWriteState.segmentInfo.name + VECTOR_BLOB_FILE_EXTENSION;
        blobContainer.writeBlob(blobName, vectorStream, (long) knnVectorValues.bytesPerVector() * totalLiveDocs, false);

        // Write Doc IDs
        InputStream docIdStream = new DocIdInputStream(knnVectorValuesSupplier.get());
        blobName = fieldInfo.getName() + "_" + segmentWriteState.segmentInfo.name + DOC_ID_FILE_EXTENSION;
        blobContainer.writeBlob(blobName, docIdStream, 4L * totalLiveDocs, false);
    }

    private void downloadFiles(FieldInfo fieldInfo, SegmentWriteState segmentWriteState) throws IOException {
        BlobContainer blobContainer = getRepository().blobStore()
            .blobContainer(getRepository().basePath().add(RemoteIndexBuilder.VECTOR_PATH));

        InputStream graphStream = blobContainer.readBlob(
            fieldInfo.getName() + "_" + segmentWriteState.segmentInfo.name + GRAPH_FILE_EXTENSION
        );

        final String engineFileName = buildEngineFileName(
            segmentWriteState.segmentInfo.name,
            KNNEngine.FAISS.getVersion(),
            fieldInfo.name,
            KNNEngine.FAISS.getExtension()
        );
        IndexOutput indexOutput = segmentWriteState.directory.createOutput(engineFileName, segmentWriteState.context);
        int bytes = graphStream.available();
        indexOutput.writeBytes(graphStream.readAllBytes(), bytes);
        CodecUtil.writeFooter(indexOutput);
    }

    private void triggerBuild(FieldInfo fieldInfo, SegmentWriteState segmentWriteState) throws IOException {
        // For now, this method just copies the existing graph to the remote repo, then deletes the existing graph.
        BlobContainer blobContainer = getRepository().blobStore()
            .blobContainer(getRepository().basePath().add(RemoteIndexBuilder.VECTOR_PATH));
        final String engineFileName = buildEngineFileName(
            segmentWriteState.segmentInfo.name,
            KNNEngine.FAISS.getVersion(),
            fieldInfo.name,
            KNNEngine.FAISS.getExtension()
        );

        IndexInput indexInput = segmentWriteState.directory.openInput(engineFileName, segmentWriteState.context);
        InputStream testStream = new TestInputStream(indexInput);
        log.info("Write Graph To Repo");
        blobContainer.writeBlob(
            fieldInfo.getName() + "_" + segmentWriteState.segmentInfo.name + GRAPH_FILE_EXTENSION,
            testStream,
            indexInput.length(),
            false
        );
        // Delete graph file off of directory so that we can read from repository later
        log.info("Delete Engine File {}: ", engineFileName);
        segmentWriteState.directory.deleteFile(engineFileName);
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
