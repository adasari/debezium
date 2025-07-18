/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import static io.debezium.util.Loggings.maybeRedactSensitiveData;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.SingleThreadAccess;
import io.debezium.config.ConfigurationDefaults;
import io.debezium.pipeline.Sizeable;
import io.debezium.time.Temporals;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.LoggingContext.PreviousContext;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

public class ChangeEventQueueManager<T extends Sizeable> implements ChangeEventQueueMetrics {
    // TODO: Introduce new metrics to replace ChangeEventQueueMetrics, enabling support for different queue delegate types
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventQueue.class);

    private final QueueProvider<T> queueProvider;
    private final Duration pollInterval;
    private final int maxBatchSize;
    private final int maxQueueSize;
    private final Supplier<PreviousContext> loggingContextSupplier;

    private final Lock lock;
    private final Condition isFull;
    private final Condition isNotFull;

    @SingleThreadAccess("producer thread")
    private boolean buffering;
    private final AtomicReference<T> bufferedEvent = new AtomicReference<>();

    private volatile RuntimeException producerException;

    private ChangeEventQueueManager(Duration pollInterval, int maxQueueSize, int maxBatchSize, Supplier<LoggingContext.PreviousContext> loggingContextSupplier,
                                    QueueProvider<T> queueProvider, boolean buffering) {
        this.pollInterval = pollInterval;
        this.maxBatchSize = maxBatchSize;
        this.maxQueueSize = maxQueueSize;
        this.queueProvider = queueProvider;
        this.loggingContextSupplier = loggingContextSupplier;
        this.buffering = buffering;

        this.lock = new ReentrantLock();
        this.isFull = lock.newCondition();
        this.isNotFull = lock.newCondition();
    }

    @Override
    public int totalCapacity() {
        return 0;
    }

    @Override
    public int remainingCapacity() {
        return 0;
    }

    @Override
    public long maxQueueSizeInBytes() {
        return 0;
    }

    @Override
    public long currentQueueSizeInBytes() {
        return 0;
    }

    public static class Builder<T extends Sizeable> {

        private Duration pollInterval;
        private Supplier<LoggingContext.PreviousContext> loggingContextSupplier;
        private boolean buffering;
        private QueueProvider<T> queueProvider;
        private int maxQueueSize;
        private int maxBatchSize;

        public ChangeEventQueueManager.Builder<T> pollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
            return this;
        }

        public ChangeEventQueueManager.Builder<T> loggingContextSupplier(Supplier<LoggingContext.PreviousContext> loggingContextSupplier) {
            this.loggingContextSupplier = loggingContextSupplier;
            return this;
        }

        public ChangeEventQueueManager.Builder<T> buffering() {
            this.buffering = true;
            return this;
        }

        public ChangeEventQueueManager.Builder<T> queueDelegate(QueueProvider<T> queueProvider) {
            this.queueProvider = queueProvider;
            return this;
        }

        public ChangeEventQueueManager.Builder<T> maxQueueSize(int maxQueueSize) {
            this.maxQueueSize = maxQueueSize;
            return this;
        }

        public ChangeEventQueueManager.Builder<T> maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public ChangeEventQueueManager<T> build() {
            return new ChangeEventQueueManager<T>(pollInterval, maxQueueSize, maxBatchSize, loggingContextSupplier, queueProvider, buffering);
        }
    }

    /**
     * Enqueues a record so that it can be obtained via {@link #poll()}. This method
     * will block if the queue is full.
     *
     * @param record
     *            the record to be enqueued
     * @throws InterruptedException
     *             if this thread has been interrupted
     */
    public void enqueue(T record) throws InterruptedException {
        if (record == null) {
            return;
        }

        // The calling thread has been interrupted, let's abort
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        if (buffering) {
            record = bufferedEvent.getAndSet(record);
            if (record == null) {
                // Can happen only for the first coming event
                return;
            }
        }

        doEnqueue(record);
    }

    /**
     * Applies a function to the event and the buffer and adds it to the queue. Buffer is emptied.
     *
     * @param recordModifier
     * @throws InterruptedException
     */
    public void flushBuffer(Function<T, T> recordModifier) throws InterruptedException {
        assert buffering : "Unsupported for queues with disabled buffering";
        T record = bufferedEvent.getAndSet(null);
        if (record != null) {
            doEnqueue(recordModifier.apply(record));
        }
    }

    /**
     * Disable buffering for the queue
     */
    public void disableBuffering() {
        assert bufferedEvent.get() == null : "Buffer must be flushed";
        buffering = false;
    }

    /**
     * Enable buffering for the queue
     */
    public void enableBuffering() {
        buffering = true;
    }

    protected void doEnqueue(T record) throws InterruptedException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Enqueuing source record '{}'", maybeRedactSensitiveData(record));
        }

        try {
            this.lock.lock();
            while (queueProvider.size() >= maxQueueSize) {
                // signal poll() to drain queue
                this.isFull.signalAll();
                // queue size or queue sizeInBytes threshold reached, so wait a bit
                this.isNotFull.await(pollInterval.toMillis(), TimeUnit.MILLISECONDS);
            }

            queueProvider.enqueue(record);

            if (queueProvider.size() >= maxBatchSize) {
                // signal poll() to start draining queue and do not wait
                this.isFull.signalAll();
            }

        }
        finally {
            this.lock.unlock();
        }

        queueProvider.enqueue(record);
    }

    /**
     * Returns the next batch of elements from this queue. May be empty in case no
     * elements have arrived in the maximum waiting time.
     *
     * @throws InterruptedException
     *             if this thread has been interrupted while waiting for more
     *             elements to arrive
     */
    public List<T> poll() throws InterruptedException {
        LoggingContext.PreviousContext previousContext = loggingContextSupplier.get();
        try {
            LOGGER.debug("polling records...");
            final Timer timeout = Threads.timer(Clock.SYSTEM, Temporals.min(pollInterval, ConfigurationDefaults.RETURN_CONTROL_INTERVAL));

            try {
                this.lock.lock();
                List<T> records = new ArrayList<>(Math.min(maxBatchSize, queueProvider.size()));
                throwProducerExceptionIfPresent();
                while (drainRecords(records, maxBatchSize - records.size()) < maxBatchSize
                        && !timeout.expired()) {
                    throwProducerExceptionIfPresent();

                    LOGGER.debug("no records available or batch size not reached yet, sleeping a bit...");
                    long remainingTimeoutMills = timeout.remaining().toMillis();
                    if (remainingTimeoutMills > 0) {
                        // signal doEnqueue() to add more records
                        this.isNotFull.signalAll();
                        // no records available or batch size not reached yet, so wait a bit
                        this.isFull.await(remainingTimeoutMills, TimeUnit.MILLISECONDS);
                    }
                    LOGGER.debug("checking for more records...");
                }
                // signal doEnqueue() to add more records
                this.isNotFull.signalAll();
                return records;
            }
            finally {
                this.lock.unlock();
            }

        }
        finally {
            previousContext.restore();
        }
    }

    private long drainRecords(List<T> records, int maxElements) throws InterruptedException {
        int queueSize = queueProvider.size();
        if (queueSize == 0) {
            return records.size();
        }
        int recordsToDrain = Math.min(queueSize, maxElements);
        T[] drainedRecords = (T[]) new Sizeable[recordsToDrain];
        for (int i = 0; i < recordsToDrain; i++) {
            T record = queueProvider.poll();
            drainedRecords[i] = record;
        }

        records.addAll(Arrays.asList(drainedRecords));
        return records.size();
    }

    public void producerException(final RuntimeException producerException) {
        this.producerException = producerException;
    }

    private void throwProducerExceptionIfPresent() {
        if (producerException != null) {
            throw producerException;
        }
    }

    public boolean isBuffered() {
        return buffering;
    }
}
