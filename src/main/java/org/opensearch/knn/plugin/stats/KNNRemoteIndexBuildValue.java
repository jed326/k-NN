/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.plugin.stats;

import lombok.Getter;

import java.util.concurrent.atomic.LongAdder;

public enum KNNRemoteIndexBuildValue {

    // Repository Stats
    WRITE_SUCCESS_COUNT("write_success_count"),
    WRITE_FAILURE_COUNT("write_failure_count"),
    WRITE_SIZE("write_size_in_bytes"),
    WRITE_TIME("write_time_in_millis"),
    READ_SUCCESS_COUNT("read_success_count"),
    READ_FAILURE_COUNT("read_failure_count"),
    READ_SIZE("read_size_in_bytes"),
    READ_TIME("read_time_in_millis"),

    BUILD_REQUEST_SUCCESS_COUNT("build_request_success_count"),
    BUILD_REQUEST_FAILURE_COUNT("build_request_failure_count"),
    STATUS_REQUEST_SUCCESS_COUNT("status_request_success_count"),
    STATUS_REQUEST_FAILURE_COUNT("status_request_failure_count"),
    INDEX_BUILD_SUCCESS_COUNT("index_build_success_count"),
    INDEX_BUILD_FAILURE_COUNT("index_build_failure_count"),
    WAITING_TIME("waiting_time_in_ms"),

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
