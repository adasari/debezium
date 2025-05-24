package io.debezium.connector.base;

import io.debezium.spi.common.Configurable;

import java.util.List;
import java.util.Optional;

public interface ChangeEventQueueDelegate<T> extends Configurable {

    void enqueue(T event) throws InterruptedException;
    List<T> poll() throws InterruptedException;
    int size();
    void close();
    boolean isClosed();

    default Optional<ChangeEventQueueMetricsProvider> getMetricsProvider() {
        return Optional.empty();
    }
}
