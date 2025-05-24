/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import io.debezium.annotation.SingleThreadAccess;
import io.debezium.annotation.ThreadSafe;
import io.debezium.config.ConfigurationDefaults;
import io.debezium.pipeline.Sizeable;
import io.debezium.time.Temporals;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.LoggingContext.PreviousContext;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.debezium.util.Loggings.maybeRedactSensitiveData;

/**
 * A queue which serves as handover point between producer threads (e.g. MySQL's
 * binlog reader thread) and the Kafka Connect polling loop.
 * <p>
 * The queue is configurable in different aspects, e.g. its maximum size and the
 * time to sleep (block) between two subsequent poll calls. See the
 * {@link Builder} for the different options. The queue applies back-pressure
 * semantics, i.e. if it holds the maximum number of elements, subsequent calls
 * to {@link #enqueue(T)} will block until elements have been removed from
 * the queue.
 * <p>
 * If an exception occurs on the producer side, the producer should make that
 * exception known by calling {@link #producerException(RuntimeException)} before stopping its
 * operation. Upon the next call to {@link #poll()}, that exception will be
 * raised, causing Kafka Connect to stop the connector and mark it as
 * {@code FAILED}.
 *
 * @author Gunnar Morling
 *
 * @param <T>
 *            the type of events in this queue. Usually {@link Sizeable} is
 *            used, but in cases where additional metadata must be passed from
 *            producers to the consumer, a custom type extending source records
 *            may be used.
 */
@ThreadSafe
public class ChangeEventQueue<T extends Sizeable> implements ChangeEventQueueMetrics {
    // TODO: Introduce new metrics to replace ChangeEventQueueMetrics, enabling support for different queue delegate types
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventQueue.class);

    private final ChangeEventQueueDelegate<T> delegate;
    private final Duration pollInterval;
    private final Supplier<PreviousContext> loggingContextSupplier;

    @SingleThreadAccess("producer thread")
    private boolean buffering;
    private final AtomicReference<T> bufferedEvent = new AtomicReference<>();

    private volatile RuntimeException producerException;

    private ChangeEventQueue(Duration pollInterval, Supplier<LoggingContext.PreviousContext> loggingContextSupplier,
                             ChangeEventQueueDelegate<T> delegate, boolean buffering) {
        this.pollInterval = pollInterval;
        this.delegate = delegate;
        this.loggingContextSupplier = loggingContextSupplier;
        this.buffering = buffering;
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
        private ChangeEventQueueDelegate<T> delegate;

        public ChangeEventQueue.Builder<T> pollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
            return this;
        }

        public ChangeEventQueue.Builder<T> loggingContextSupplier(Supplier<LoggingContext.PreviousContext> loggingContextSupplier) {
            this.loggingContextSupplier = loggingContextSupplier;
            return this;
        }

        public ChangeEventQueue.Builder<T> buffering() {
            this.buffering = true;
            return this;
        }

        public ChangeEventQueue.Builder<T> queueDelegate(ChangeEventQueueDelegate<T> delegate) {
            this.delegate = delegate;
            return this;
        }

        public ChangeEventQueue<T> build() {
            return new ChangeEventQueue<T>(pollInterval, loggingContextSupplier, delegate, buffering);
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

        delegate.enqueue(record);
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
            return null;
        }
        finally {
            previousContext.restore();
        }
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
