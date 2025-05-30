/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.config.CommonConnectorConfig.*;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.DefaultChangeEventQueue;
import io.debezium.util.LoggingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Common multi-adapter streaming metrics tests.
 */
public abstract class OracleStreamingMetricsTest<T extends AbstractOracleStreamingChangeEventSourceMetrics> {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    protected OracleConnectorConfig connectorConfig;
    protected T metrics;
    protected Clock fixedClock;

    @Before
    public void before() {
        init(TestHelper.defaultConfig());
    }

    protected void init(Configuration.Builder builder) {
        this.connectorConfig = new OracleConnectorConfig(builder.build());

        Properties props = new Properties();
        props.put(CommonConnectorConfig.MAX_BATCH_SIZE, DEFAULT_MAX_BATCH_SIZE);
        props.put(CommonConnectorConfig.MAX_QUEUE_SIZE, DEFAULT_MAX_QUEUE_SIZE);
        final ChangeEventQueue<DataChangeEvent> queue = new DefaultChangeEventQueue<>(Configuration.from(props), () -> LoggingContext.forConnector("a", "b", "c"));

        final OracleTaskContext taskContext = mock(OracleTaskContext.class);
        Mockito.when(taskContext.getConnectorName()).thenReturn("connector name");
        Mockito.when(taskContext.getConnectorType()).thenReturn("connector type");

        final OracleEventMetadataProvider metadataProvider = new OracleEventMetadataProvider();
        fixedClock = Clock.fixed(Instant.parse("2021-05-15T12:30:00.00Z"), ZoneOffset.UTC);

        this.metrics = createMetrics(taskContext, queue, metadataProvider, connectorConfig, fixedClock);
    }

    protected abstract T createMetrics(OracleTaskContext taskContext,
                                       ChangeEventQueue<DataChangeEvent> queue,
                                       EventMetadataProvider metadataProvider,
                                       OracleConnectorConfig connectorConfig,
                                       Clock clock);
}
