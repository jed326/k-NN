/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.remote;

import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.util.function.Supplier;

public class RemoteIndexBuilder extends AbstractLifecycleComponent {

    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    static final String VECTOR_REPO = "vector-repo";
    public static final String VECTOR_PATH = "vectors";

    public RemoteIndexBuilder(Supplier<RepositoriesService> repositoriesServiceSupplier) {
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
    }

    /**
     * Gets the KNN repository container from the repository service which we can read/write to
     */
    public BlobStoreRepository getRepository() {
        RepositoriesService repositoriesService = repositoriesServiceSupplier.get();
        final Repository repository = repositoriesService.repository(VECTOR_REPO);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        return (BlobStoreRepository) repository;
    }

    /**
     * Writes some data to the repository
     */
    public void writeFiles() {

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
