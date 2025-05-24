package io.debezium.connector.base;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DefaultChangeEventQueueDelegate<T> implements ChangeEventQueueDelegate<T> {

    private BlockingQueue<T> queue;
    private volatile boolean closed = false;

    @Override
    public void configure(Map<String, ?> properties) {
        this.queue = new LinkedBlockingQueue<>();
    }

    @Override
    public void enqueue(T event) throws InterruptedException {
        queue.add(event);
    }

    @Override
    public List<T> poll() throws InterruptedException {
        return List.of();
    }

    @Override
    public int size() {
        return 0;
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
