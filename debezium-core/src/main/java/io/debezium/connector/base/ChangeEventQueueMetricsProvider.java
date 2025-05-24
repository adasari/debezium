package io.debezium.connector.base;

public interface ChangeEventQueueMetricsProvider {

    int getCurrentSize();
}
