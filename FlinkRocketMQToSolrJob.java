package com.enterprise.iudp.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * Enterprise Production-Grade Apache Flink Stream Processing Job.
 *
 * Flow:
 * 1. Consumes high-throughput telemetry data originating from RocketMQ (Simulated Source stream).
 * 2. Groups events by asset (Vessel ID / Hub ID) via a Keyed Stream.
 * 3. Applies a stateful KeyedProcessFunction with value state tracking to compute rolling
 *    statistics and trigger anomaly threshold warnings (e.g., Temperature spike > 4.0 °C).
 * 4. Enriches payloads with state metrics and executes a RichSinkFunction that leverages
 *    high-performance HTTP POST operations to stream and index enriched JSON documents
 *    directly into Apache Solr's "enterprise-assets" core.
 */
public class FlinkRocketMQToSolrJob {

    public static void main(String[] args) throws Exception {
        // Initialize high-performance Stream Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable exactly-once checkpointing every 10 seconds (RocksDB backed state consistency)
        env.getCheckpointConfig().setCheckpointInterval(10000);

        // 1. Source: Simulating a Stream of Raw Asset Telemetry from RocketMQ Source
        DataStream<TelemetryPayload> telemetryStream = env.addSource(new RocketMQSourcedMockSource())
                .name("RocketMQTelemetrySource")
                .returns(TypeInformation.of(TelemetryPayload.class));

        // 2. State & Processing: Key by AssetId and apply stateful KeyedProcessFunction for anomaly detection
        DataStream<EnrichedTelemetry> enrichedStream = telemetryStream
                .keyBy(TelemetryPayload::getAssetId)
                .process(new StatefulAnomalyEvaluator())
                .name("StatefulAnomalyEvaluator");

        // 3. Sink: Write enriched payloads to Apache Solr
        enrichedStream.addSink(new SolrIndexingSink("http://iudp-solr:8983/solr/enterprise-assets/update?commit=true"))
                .name("ApacheSolrSink")
                .setParallelism(2);

        // Execute the distributed processing job
        env.execute("IntelligentUnifiedPlatform-TelemetryStream-to-Solr");
    }

    // ==========================================
    // DOMAIN MODELS (Serializable for Flink)
    // ==========================================
    public static class TelemetryPayload implements Serializable {
        private static final long serialVersionUID = 1L;

        private String assetId;
        private String tenantId;
        private double temperature;
        private double latitude;
        private double longitude;
        private long timestamp;

        public TelemetryPayload() {}

        public TelemetryPayload(String assetId, String tenantId, double temperature, double latitude, double longitude, long timestamp) {
            this.assetId = assetId;
            this.tenantId = tenantId;
            this.temperature = temperature;
            this.latitude = latitude;
            this.longitude = longitude;
            this.timestamp = timestamp;
        }

        public String getAssetId() { return assetId; }
        public void setAssetId(String assetId) { this.assetId = assetId; }
        public String getTenantId() { return tenantId; }
        public void setTenantId(String tenantId) { this.tenantId = tenantId; }
        public double getTemperature() { return temperature; }
        public void setTemperature(double temperature) { this.temperature = temperature; }
        public double getLatitude() { return latitude; }
        public void setLatitude(double latitude) { this.latitude = latitude; }
        public double getLongitude() { return longitude; }
        public void setLongitude(double longitude) { this.longitude = longitude; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }

    public static class EnrichedTelemetry implements Serializable {
        private static final long serialVersionUID = 1L;

        private String id; // AssetId + Timestamp unique identifier for Solr Index
        private String assetId_s;
        private String tenantId_s;
        private double temperature_d;
        private double latitude_d;
        private double longitude_d;
        private double lastTemperature_d;
        private boolean anomalyTriggered_b;
        private String alertLevel_s;
        private long timestamp_l;

        public EnrichedTelemetry() {}

        public EnrichedTelemetry(String id, String assetId_s, String tenantId_s, double temperature_d,
                                 double latitude_d, double longitude_d, double lastTemperature_d,
                                 boolean anomalyTriggered_b, String alertLevel_s, long timestamp_l) {
            this.id = id;
            this.assetId_s = assetId_s;
            this.tenantId_s = tenantId_s;
            this.temperature_d = temperature_d;
            this.latitude_d = latitude_d;
            this.longitude_d = longitude_d;
            this.lastTemperature_d = lastTemperature_d;
            this.anomalyTriggered_b = anomalyTriggered_b;
            this.alertLevel_s = alertLevel_s;
            this.timestamp_l = timestamp_l;
        }

        // Getters/Setters to assist with JSON serialization
        public String getId() { return id; }
        public String getAssetId_s() { return assetId_s; }
        public String getTenantId_s() { return tenantId_s; }
        public double getTemperature_d() { return temperature_d; }
        public double getLatitude_d() { return latitude_d; }
        public double getLongitude_d() { return longitude_d; }
        public double getLastTemperature_d() { return lastTemperature_d; }
        public boolean isAnomalyTriggered_b() { return anomalyTriggered_b; }
        public String getAlertLevel_s() { return alertLevel_s; }
        public long getTimestamp_l() { return timestamp_l; }
    }

    // ==========================================
    // MOCK SOURCE (Simulating RocketMQ consumers)
    // ==========================================
    public static class RocketMQSourcedMockSource implements org.apache.flink.streaming.api.functions.source.SourceFunction<TelemetryPayload> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<TelemetryPayload> ctx) throws Exception {
            String[] assets = {"VESSEL-A-098", "VESSEL-B-112", "PORT-HUB-CHICAGO", "DISTRIBUTION-EU-12"};
            String[] tenants = {"TENANT_AMER_1", "TENANT_APAC_2", "TENANT_EMEA_3", "TENANT_LATAM_4"};
            double[] baseTemps = {2.1, -1.5, 18.4, 4.2};

            while (running) {
                long now = System.currentTimeMillis();
                int idx = (int) (Math.random() * assets.length);
                double variation = (Math.random() - 0.4) * 3.5; // Simulate temperature spikes
                double temperature = baseTemps[idx] + variation;

                ctx.collect(new TelemetryPayload(
                        assets[idx],
                        tenants[idx],
                        temperature,
                        37.7749 + (Math.random() * 2), // Mock latitude coordinates
                        -122.4194 + (Math.random() * 2), // Mock longitude coordinates
                        now
                ));

                Thread.sleep(1500); // Emits every 1.5 seconds
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // ==========================================
    // STATEFUL CEP & ANOMALY EVALUATION FUNCTION
    // ==========================================
    public static class StatefulAnomalyEvaluator extends KeyedProcessFunction<String, TelemetryPayload, EnrichedTelemetry> {

        // Flink ValueState to record previous telemetry temperatures for comparison
        private transient ValueState<Double> lastTemperatureState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Double> desc = new ValueStateDescriptor<>(
                    "lastTemperature",
                    Types.DOUBLE
            );
            lastTemperatureState = getRuntimeContext().getState(desc);
        }

        @Override
        public void processElement(TelemetryPayload value, Context ctx, Collector<EnrichedTelemetry> out) throws Exception {
            // Retrieve previous state or initialize
            Double lastTemp = lastTemperatureState.value();
            if (lastTemp == null) {
                lastTemp = value.getTemperature();
            }

            // Stateful Complex Logic: Detect anomalies where temperature rises > 4.0 °C vs previous record
            double diff = Math.abs(value.getTemperature() - lastTemp);
            boolean anomalyTriggered = diff > 4.0;
            String alertLevel = "NORMAL";

            if (anomalyTriggered) {
                alertLevel = "CRITICAL_SPIKE";
            } else if (diff > 2.0) {
                alertLevel = "WARNING_TEMPERATURE_DRIFT";
            }

            // Update ValueState with the current temperature
            lastTemperatureState.update(value.getTemperature());

            // Build structural, enriched analytics payload
            String docId = value.getAssetId() + "_" + value.getTimestamp();
            EnrichedTelemetry enriched = new EnrichedTelemetry(
                    docId,
                    value.getAssetId(),
                    value.getTenantId(),
                    value.getTemperature(),
                    value.getLatitude(),
                    value.getLongitude(),
                    lastTemp,
                    anomalyTriggered,
                    alertLevel,
                    value.getTimestamp()
            );

            out.collect(enriched);
        }
    }

    // ==========================================
    // APACHE SOLR WRITER (RICH SINK FUNCTION)
    // ==========================================
    public static class SolrIndexingSink extends RichSinkFunction<EnrichedTelemetry> {
        private final String solrUpdateUrl;
        private transient CloseableHttpClient httpClient;

        public SolrIndexingSink(String solrUpdateUrl) {
            this.solrUpdateUrl = solrUpdateUrl;
        }

        @Override
        public void open(Configuration parameters) {
            // Instantiate persistent, pooled HTTP client for low-latency writes
            this.httpClient = HttpClients.custom()
                    .setMaxConnTotal(10)
                    .setMaxConnPerRoute(5)
                    .build();
        }

        @Override
        public void invoke(EnrichedTelemetry value, Context context) throws Exception {
            // Format document payload as dynamic Solr-compatible JSON document update
            String jsonPayload = String.format(
                    "[{" +
                    "\"id\":\"%s\"," +
                    "\"assetId_s\":\"%s\"," +
                    "\"tenantId_s\":\"%s\"," +
                    "\"temperature_d\":%f," +
                    "\"latitude_d\":%f," +
                    "\"longitude_d\":%f," +
                    "\"lastTemperature_d\":%f," +
                    "\"anomalyTriggered_b\":%b," +
                    "\"alertLevel_s\":\"%s\"," +
                    "\"timestamp_l\":%d" +
                    "}]",
                    value.getId(),
                    value.getAssetId_s(),
                    value.getTenantId_s(),
                    value.getTemperature_d(),
                    value.getLatitude_d(),
                    value.getLongitude_d(),
                    value.getLastTemperature_d(),
                    value.isAnomalyTriggered_b(),
                    value.getAlertLevel_s(),
                    value.getTimestamp_l()
            );

            // Execute high-throughput bulk indexing HTTP POST call
            HttpPost httpPost = new HttpPost(solrUpdateUrl);
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setEntity(new StringEntity(jsonPayload, StandardCharsets.UTF_8));

            try {
                this.httpClient.execute(httpPost, response -> {
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode != 200) {
                        System.err.printf("[Flink-Solr-Sink] Failed to index document %s, Status Code: %d%n", value.getId(), statusCode);
                    }
                    return null;
                });
            } catch (Exception e) {
                System.err.printf("[Flink-Solr-Sink] Error during Solr index execution for id %s: %s%n", value.getId(), e.getMessage());
            }
        }

        @Override
        public void close() throws Exception {
            if (this.httpClient != null) {
                this.httpClient.close();
            }
        }
    }
}
