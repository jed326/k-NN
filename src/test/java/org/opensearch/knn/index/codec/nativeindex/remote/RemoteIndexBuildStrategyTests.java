/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.nativeindex.remote;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.codec.nativeindex.NativeIndexBuildStrategy;
import org.opensearch.knn.index.codec.nativeindex.model.BuildIndexParams;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.store.IndexOutputWithBuffer;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.knn.index.vectorvalues.KNNVectorValuesFactory;
import org.opensearch.knn.index.vectorvalues.TestVectorValues;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.knn.index.KNNSettings.KNN_REMOTE_VECTOR_REPO_SETTING;

public class RemoteIndexBuildStrategyTests extends KNNTestCase {

    static int fallbackCounter = 0;

    private static class TestIndexBuildStrategy implements NativeIndexBuildStrategy {

        @Override
        public void buildAndWriteIndex(BuildIndexParams indexInfo) throws IOException {
            fallbackCounter++;
        }
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ClusterSettings clusterSettings = mock(ClusterSettings.class);
        when(clusterSettings.get(KNN_REMOTE_VECTOR_REPO_SETTING)).thenReturn("test-repo-name");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        KNNSettings.state().setClusterService(clusterService);
    }

    public void testFallback() throws IOException {
        List<float[]> vectorValues = List.of(new float[] { 1, 2 }, new float[] { 2, 3 }, new float[] { 3, 4 });
        final TestVectorValues.PreDefinedFloatVectorValues randomVectorValues = new TestVectorValues.PreDefinedFloatVectorValues(
            vectorValues
        );
        final KNNVectorValues<byte[]> knnVectorValues = KNNVectorValuesFactory.getVectorValues(VectorDataType.FLOAT, randomVectorValues);

        RepositoriesService repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.repository(any())).thenThrow(new RepositoryMissingException("Fallback"));

        RemoteIndexBuildStrategy objectUnderTest = new RemoteIndexBuildStrategy(() -> repositoriesService, new TestIndexBuildStrategy());

        IndexOutputWithBuffer indexOutputWithBuffer = Mockito.mock(IndexOutputWithBuffer.class);

        BuildIndexParams buildIndexParams = BuildIndexParams.builder()
            .indexOutputWithBuffer(indexOutputWithBuffer)
            .knnEngine(KNNEngine.FAISS)
            .vectorDataType(VectorDataType.FLOAT)
            .parameters(Map.of("index", "param"))
            .knnVectorValuesSupplier(() -> knnVectorValues)
            .totalLiveDocs((int) knnVectorValues.totalLiveDocs())
            .build();

        objectUnderTest.buildAndWriteIndex(buildIndexParams);
        assertEquals(1, fallbackCounter);
    }

    /**
     * Verify the buffered read method in {@link RemoteIndexBuildStrategy#readFromRepository} produces the correct result
     */
    public void testRepositoryRead() throws IOException {
        String TEST_FILE_NAME = randomAlphaOfLength(8) + KNNEngine.FAISS.getExtension();

        // Create an InputStream with random values
        int TEST_ARRAY_SIZE = 64 * 1024 * 10;
        byte[] byteArray = new byte[TEST_ARRAY_SIZE];
        Random random = new Random();
        random.nextBytes(byteArray);
        InputStream randomStream = new ByteArrayInputStream(byteArray);

        // Create a test segment that we will read/write from
        Directory directory;
        directory = newFSDirectory(createTempDir());
        String TEST_SEGMENT_NAME = "test-segment-name";
        IndexOutput testIndexOutput = directory.createOutput(TEST_SEGMENT_NAME, IOContext.DEFAULT);
        IndexOutputWithBuffer testIndexOutputWithBuffer = new IndexOutputWithBuffer(testIndexOutput);

        // Set up RemoteIndexBuildStrategy and write to IndexOutput
        RepositoriesService repositoriesService = mock(RepositoriesService.class);
        BlobStoreRepository mockRepository = mock(BlobStoreRepository.class);
        BlobPath testBasePath = new BlobPath().add("testBasePath");
        BlobStore mockBlobStore = mock(BlobStore.class);
        AsyncMultiStreamBlobContainer mockBlobContainer = mock(AsyncMultiStreamBlobContainer.class);

        when(repositoriesService.repository(any())).thenReturn(mockRepository);
        when(mockRepository.basePath()).thenReturn(testBasePath);
        when(mockRepository.blobStore()).thenReturn(mockBlobStore);
        when(mockBlobStore.blobContainer(any())).thenReturn(mockBlobContainer);
        when(mockBlobContainer.readBlob(TEST_FILE_NAME)).thenReturn(randomStream);

        RemoteIndexBuildStrategy objectUnderTest = new RemoteIndexBuildStrategy(
            () -> repositoriesService,
            mock(NativeIndexBuildStrategy.class)
        );

        // Verify file extension check
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.readFromRepository("test_file.txt", testIndexOutputWithBuffer));

        // Now test with valid file extensions
        String testPath = randomFrom(
            List.of(
                "testBasePath/testDirectory/" + TEST_FILE_NAME, // Test with subdirectory
                "testBasePath/" + TEST_FILE_NAME, // Test with only base path
                TEST_FILE_NAME // test with no base path
            )
        );
        // This should read from randomStream into testIndexOutput
        objectUnderTest.readFromRepository(testPath, testIndexOutputWithBuffer);
        testIndexOutput.close();

        // Now try to read from the IndexOutput
        IndexInput testIndexInput = directory.openInput(TEST_SEGMENT_NAME, IOContext.DEFAULT);
        byte[] resultByteArray = new byte[TEST_ARRAY_SIZE];
        testIndexInput.readBytes(resultByteArray, 0, TEST_ARRAY_SIZE);
        assertArrayEquals(byteArray, resultByteArray);

        // Test Cleanup
        testIndexInput.close();
        directory.close();
    }
}
