/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultQueueProvider<T> implements QueueProvider<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultQueueProvider.class);

    // private final Duration pollInterval;
    // private final int maxBatchSize;
    // private final int maxQueueSize;
    //
    // private final Lock lock;
    // private final Condition isFull;
    // private final Condition isNotFull;

    private Queue<T> queue;
    private volatile boolean closed = false;

    // public DefaultQueueProvider(Duration pollInterval, int maxQueueSize, int maxBatchSize) {
    // this.pollInterval = pollInterval;
    // this.maxBatchSize = maxBatchSize;
    // this.maxQueueSize = maxQueueSize;
    //
    // this.lock = new ReentrantLock();
    // this.isFull = lock.newCondition();
    // this.isNotFull = lock.newCondition();
    // }

    public DefaultQueueProvider(int maxQueueSize) {
        this.queue = new ArrayDeque<>(maxQueueSize);
    }

    @Override
    public void configure(Map<String, ?> properties) {
        this.queue = new ArrayDeque<>();
    }

    public void enqueue(T record) throws InterruptedException {
        queue.add(record);
    }

    // @Override
    // public void enqueue(T record) throws InterruptedException {
    // if (LOGGER.isTraceEnabled()) {
    // LOGGER.trace("Enqueuing source record '{}'", maybeRedactSensitiveData(record));
    // }
    //
    // try {
    // this.lock.lock();
    // while (queue.size() >= maxQueueSize) {
    // // signal poll() to drain queue
    // this.isFull.signalAll();
    // // queue size or queue sizeInBytes threshold reached, so wait a bit
    // this.isNotFull.await(pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    // }
    //
    // queue.add(record);
    //
    // if (queue.size() >= maxBatchSize) {
    // // signal poll() to start draining queue and do not wait
    // this.isFull.signalAll();
    // }
    //
    // } finally {
    // this.lock.unlock();
    // }
    //
    // }

    @Override
    public T poll() throws InterruptedException {
        return queue.poll();
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
