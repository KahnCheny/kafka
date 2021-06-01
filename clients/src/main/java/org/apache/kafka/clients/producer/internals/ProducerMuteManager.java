/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.io.Closeable;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * A class which maintains mute of TopicPartitions. Also keeps the number of TopicPartitions accumulated-batches and in-flight requests.
 */
public class ProducerMuteManager implements Closeable {

    private final Logger log;
    private final RecordAccumulator accumulator;
    private Map<TopicPartition, List<ProducerBatch>> inFlightBatches;

    private final Set<TopicPartition> mutedPartitions;

    public ProducerMuteManager(final RecordAccumulator accumulator, final LogContext logContext) {
        this.log = logContext.logger(ProducerMuteManager.class);

        this.accumulator = accumulator;
        this.mutedPartitions = new HashSet<>();
    }

    /**
     * Add mute of TopicPartition
     *
     * @param topicPartition
     */
    public synchronized void mute(TopicPartition topicPartition) {
        this.mutedPartitions.add(topicPartition);
    }

    /**
     * Remove muted of TopicPartition
     *
     * @param topicPartition
     */
    public synchronized void unmute(TopicPartition topicPartition) {
        this.mutedPartitions.remove(topicPartition);
    }

    public boolean isMute(TopicPartition topicPartition) {
        return this.mutedPartitions.contains(topicPartition);
    }

    /**
     *  Return muted of TopicPartitions
     *
     * @return
     */
    public Set<TopicPartition> getMutedPartitions() {
        return Collections.unmodifiableSet(mutedPartitions);
    }

    public synchronized void close() {
        if (this.mutedPartitions != null) {
            this.mutedPartitions.clear();
        }
    }

    /**
     *  Return the number of TopicPartition accumulated-batches requests
     *
     * @return
     */
    public Map<TopicPartition, Integer> getAccumulatedBatches() {
        Map<TopicPartition, Integer> accumulatedBatches = new HashMap<>();
        for (Entry<TopicPartition, Deque<ProducerBatch>> topicPartitionDeque : accumulator.batches().entrySet()) {
            accumulatedBatches.put(topicPartitionDeque.getKey(), topicPartitionDeque.getValue().size());
        }
        return accumulatedBatches;
    }

    void setInFlightBatches(Map<TopicPartition, List<ProducerBatch>> inFlightBatches) {
        this.inFlightBatches = inFlightBatches;
    }

    /**
     * Return the number of TopicPartition in-flight requests
     *
     * @return The request count.
     */
    public Map<TopicPartition, Integer> getInFlightRequestCount() {
        Map<TopicPartition, Integer> inFlightRequestCount = new HashMap<>();
        for (Entry<TopicPartition, List<ProducerBatch>> topicPartitionListEntry : this.inFlightBatches.entrySet()) {
            Integer count = 0;
            TopicPartition topicPartition = topicPartitionListEntry.getKey();
            List<ProducerBatch> producerBatchList = topicPartitionListEntry.getValue();
            for (ProducerBatch producerBatch : producerBatchList) {
                count += producerBatch.recordCount;
            }
            inFlightRequestCount.put(topicPartition, count);
        }
        return inFlightRequestCount;
    }
}
