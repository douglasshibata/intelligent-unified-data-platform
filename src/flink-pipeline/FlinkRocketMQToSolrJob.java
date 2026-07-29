package com.enterprise.data.pipeline;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.solr.SolrSink; // Conceptual Solr connector representing index pipeline
import org.apache.flink.connector.rocketmq.source.RocketMQSource; // Conceptual RocketMQ connector representation
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

/**
 * FlinkStreamToSolrJob
 *
 * Production-grade Apache Flink streaming pipeline.
 * Ingests low-latency JSON telemetry data streams from Apache RocketMQ,
 * performs stateful temporal threshold/anomaly aggregation using RocksDB state storage,
 * and sinks the enriched, indexed documents directly into an Apache Solr search cluster.
 */
public class FlinkRocketMQToSolrJob {

    public static void main(String[] args) throws Exception {
        // 1. Initialize Stream Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Ensure state backend is set to RocksDB for incremental checkpoints (configured in flink-conf.yaml / compose)
        env.getCheckpointConfig().setCheckpointInterval(10000); // 10 seconds checkpoint interval

        // 2. Define RocketMQ Source configuration (Conceptual native source integration)
        // In actual setups, this leverages rocketmq-flink-connector
        DataStream<String> telemetryRawStream = env.addSource(new RocketMQSourceMock("rocketmq-namesrv:9876", "supply_chain_telemetry"));

        // 3. Key by Vehicle/Device ID and apply stateful anomaly detection
        DataStream<EnrichedTelemetryEvent> processedStream = telemetryRawStream
            .map(new TelemetryParser())
            .keyBy(TelemetryEvent::getDeviceId)
            .map(new StatefulAnomalyEvaluator());

        // 4. Sink to Solr Search Index
        processedStream.addSink(new SolrSinkMock("http://solr:8983/solr/supply_chain_indices"));

        // 5. Execute Flink Topology
        env.execute("Flink-RocketMQ-To-Solr-Stateful-Pipeline");
    }

    // ==========================================
    // DOMAIN MODELS
    // ==========================================
    public static class TelemetryEvent implements Serializable {
        private String deviceId;
        private double temperature;
        private double latitude;
        private double longitude;
        private long timestamp;

        public TelemetryEvent() {}

        public String getDeviceId() { return deviceId; }
        public void setDeviceId(String deviceId) { this.deviceId = deviceId; }
        public double getTemperature() { return temperature; }
        public void setTemperature(double temperature) { this.temperature = temperature; }
        public double getLatitude() { return latitude; }
        public void setLatitude(double latitude) { this.latitude = latitude; }
        public double getLongitude() { return longitude; }
        public void setLongitude(double longitude) { this.longitude = longitude; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }

    public static class EnrichedTelemetryEvent extends TelemetryEvent implements Serializable {
        private boolean anomalyDetected;
        private String reason;
        private double historicalAverageTemp;

        public EnrichedTelemetryEvent(TelemetryEvent base) {
            this.setDeviceId(base.getDeviceId());
            this.setTemperature(base.getTemperature());
            this.setLatitude(base.getLatitude());
            this.setLongitude(base.getLongitude());
            this.setTimestamp(base.getTimestamp());
        }

        public boolean isAnomalyDetected() { return anomalyDetected; }
        public void setAnomalyDetected(boolean anomalyDetected) { this.anomalyDetected = anomalyDetected; }
        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }
        public double getHistoricalAverageTemp() { return historicalAverageTemp; }
        public void setHistoricalAverageTemp(double historicalAverageTemp) { this.historicalAverageTemp = historicalAverageTemp; }
    }

    // ==========================================
    // STREAM OPERATORS
    // ==========================================
    private static class TelemetryParser extends RichMapFunction<String, TelemetryEvent> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            this.mapper = new ObjectMapper();
        }

        @Override
        public TelemetryEvent map(String value) throws Exception {
            return mapper.readValue(value, TelemetryEvent.class);
        }
    }

    /**
     * StatefulAnomalyEvaluator
     *
     * Uses Flink ValueState (persisted in RocksDB state backend) to maintain running
     * stats on a per-device basis to detect rapid temperature jumps or persistent heating anomalies.
     */
    private static class StatefulAnomalyEvaluator extends RichMapFunction<TelemetryEvent, EnrichedTelemetryEvent> {
        private transient ValueState<DeviceState> deviceStateStore;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<DeviceState> descriptor = new ValueStateDescriptor<>(
                "deviceStateStore",
                TypeInformation.of(DeviceState.class)
            );
            deviceStateStore = getRuntimeContext().getState(descriptor);
        }

        @Override
        public EnrichedTelemetryEvent map(TelemetryEvent value) throws Exception {
            DeviceState state = deviceStateStore.value();
            if (state == null) {
                state = new DeviceState();
                state.count = 0;
                state.tempSum = 0.0;
            }

            // Update stats
            state.count += 1;
            state.tempSum += value.getTemperature();
            double avgTemp = state.tempSum / state.count;

            EnrichedTelemetryEvent enriched = new EnrichedTelemetryEvent(value);
            enriched.setHistoricalAverageTemp(avgTemp);

            // Anomaly logic: temperature exceeds average by 15 degrees
            if (state.count > 3 && (value.getTemperature() - avgTemp) > 15.0) {
                enriched.setAnomalyDetected(true);
                enriched.setReason("Temperature deviation exceeds threshold! Current: " + value.getTemperature() + ", Average: " + avgTemp);
            } else {
                enriched.setAnomalyDetected(false);
                enriched.setReason("Normal parameters.");
            }

            // Save updated state back to RocksDB
            deviceStateStore.update(state);
            return enriched;
        }
    }

    public static class DeviceState implements Serializable {
        public long count;
        public double tempSum;
    }

    // ==========================================
    // MOCK CLASSES FOR INTEGRATION DEFINITION
    // ==========================================
    private static class RocketMQSourceMock implements org.apache.flink.streaming.api.functions.source.SourceFunction<String> {
        private final String nsAddr;
        private final String topic;

        public RocketMQSourceMock(String nsAddr, String topic) {
            this.nsAddr = nsAddr;
            this.topic = topic;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            // Emits mock supply chain telemetry
            long counter = 0;
            while (true) {
                String payload = String.format(
                    "{\"deviceId\":\"DEV-%03d\",\"temperature\":%.2f,\"latitude\":-23.55,\"longitude\":-46.63,\"timestamp\":%d}",
                    (int) (Math.random() * 5) + 1,
                    20.0 + (Math.random() * 25.0),
                    System.currentTimeMillis()
                );
                ctx.collect(payload);
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {}
    }

    private static class SolrSinkMock implements org.apache.flink.streaming.api.functions.sink.SinkFunction<EnrichedTelemetryEvent> {
        private final String solrUrl;

        public SolrSinkMock(String solrUrl) {
            this.solrUrl = solrUrl;
        }

        @Override
        public void invoke(EnrichedTelemetryEvent value, Context context) {
            // Conceptual representation of indexing a document into SolrCloud
            System.out.println(String.format(
                "[SOLR SINK] Indexing to %s: ID=%s Temp=%.2f Anomaly=%b Reason=%s",
                solrUrl, value.getDeviceId(), value.getTemperature(), value.isAnomalyDetected(), value.getReason()
            ));
        }
    }
}
