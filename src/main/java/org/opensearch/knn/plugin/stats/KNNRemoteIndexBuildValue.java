/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.plugin.stats;

import lombok.Getter;

import java.util.concurrent.atomic.LongAdder;

public enum KNNRemoteIndexBuildValue {

    REPOSITORY_WRITE_SUCCESS_COUNT("repository_write_success_count"),
    REPOSITORY_WRITE_FAILURE_COUNT("repository_write_failure_count"),
    REPOSITORY_WRITE_SIZE("repository_write_size_in_bytes"),
    REPOSITORY_WRITE_TIME("repository_write_time_in_millis"),
    REPOSITORY_READ_SUCCESS_COUNT("repository_read_success_count"),
    REPOSITORY_READ_FAILURE_COUNT("repository_read_failure_count"),
    REPOSITORY_READ_SIZE("repository_read_size_in_bytes"),
    REPOSITORY_READ_TIME("repository_read_time_in_millis"),
    REPOSITORY_PARALLEL_UPLOAD_SUCCESS_COUNT("repository_parallel_upload_success_count"),
    REPOSITORY_PARALLEL_UPLOAD_FAILURE_COUNT("repository_parallel_upload_failure_count"),
    REPOSITORY_SEQUENTIAL_UPLOAD_SUCCESS_COUNT("repository_sequential_upload_success_count"),
    REPOSITORY_SEQUENTIAL_UPLOAD_FAILURE_COUNT("repository_sequential_upload_failure_count"),

    BUILD_REQUEST_SUCCESS_COUNT("build_request_success_count"),
    BUILD_REQUEST_FAILURE_COUNT("build_request_failure_count"),
    STATUS_REQUEST_SUCCESS_COUNT("status_request_success_count"),
    STATUS_REQUEST_FAILURE_COUNT("status_request_failure_count"),
    INDEX_BUILD_SUCCESS_COUNT("index_build_success_count"),
    INDEX_BUILD_FAILURE_COUNT("index_build_failure_count"),

    REMOTE_FLUSH_TIME("remote_flush_time_in_millis"),
    REMOTE_MERGE_TIME("remote_merge_time_in_millis");

    @Getter
    private final String name;
    private final LongAdder value;

    /**
     * Constructor
     *
     * @param name name of the graph value
     */
    KNNRemoteIndexBuildValue(String name) {
        this.name = name;
        this.value = new LongAdder();
    }

    /**
     * Get the graph value
     *
     * @return value
     */
    public Long getValue() {
        return value.longValue();
    }

    /**
     * Increment the graph value
     */
    public void increment() {
        value.increment();
    }

    /**
     * Decrement the graph value
     */
    public void decrement() {
        value.decrement();
    }

    /**
     * Increment the graph value by a specified amount
     *
     * @param delta The amount to increment
     */
    public void incrementBy(long delta) {
        value.add(delta);
    }

    /**
     * Decrement the graph value by a specified amount
     *
     * @param delta The amount to decrement
     */
    public void decrementBy(long delta) {
        value.add(delta * -1);
    }
}
