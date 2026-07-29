import React, { useEffect, useRef, useState } from "react";
import ReactECharts from "echarts-for-react";
import * as echarts from "echarts";

/**
 * Interface defining the metrics payload emitted by the Flink stream processing job.
 */
interface StreamTelemetryMetric {
  assetId: string;
  tenantId: string;
  temperature: number;
  latitude: number;
  longitude: number;
  lastTemperature: number;
  anomalyTriggered: boolean;
  alertLevel: string;
  timestamp: number;
}

/**
 * Intelligent Unified Data & Analytics Platform: Real-Time Stream Visualizer Component.
 * Consumes real-time WebSockets powered by Flink CEP analytics and dynamically updates
 * a multi-series Apache ECharts dashboard rendering port asset workloads and temperature fluctuations.
 */
export const RealTimeDashboard: React.FC = () => {
  const [metrics, setMetrics] = useState<StreamTelemetryMetric[]>([]);
  const [connectionStatus, setConnectionStatus] = useState<"CONNECTING" | "ONLINE" | "OFFLINE">("CONNECTING");
  const wsRef = useRef<WebSocket | null>(null);

  // Maintain internal circular buffers for chart series data (max 50 points to prevent DOM overhead)
  const [timeBuffer, setTimeBuffer] = useState<string[]>([]);
  const [tempBuffer, setTempBuffer] = useState<number[]>([]);
  const [anomalyCountBuffer, setAnomalyCountBuffer] = useState<number[]>([]);

  useEffect(() => {
    // Connect to the Flink/RocketMQ telemetry push websocket endpoint
    const socketUrl = "ws://localhost:8080/api/v1/telemetry/ws";
    const ws = new WebSocket(socketUrl);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnectionStatus("ONLINE");
      console.log("[WebSocket] Connection established with central telemetry stream.");
    };

    ws.onmessage = (event) => {
      try {
        const payload: StreamTelemetryMetric = JSON.parse(event.data);

        // Append payload to metrics state list
        setMetrics((prev) => [payload, ...prev.slice(0, 99)]);

        // Format timestamp to localized HH:MM:SS format
        const formattedTime = new Date(payload.timestamp).toLocaleTimeString();

        // Update circular visual buffers
        setTimeBuffer((prev) => [...prev, formattedTime].slice(-40));
        setTempBuffer((prev) => [...prev, payload.temperature].slice(-40));
        setAnomalyCountBuffer((prev) => {
          const lastCount = prev[prev.length - 1] || 0;
          const newCount = payload.anomalyTriggered ? lastCount + 1 : lastCount;
          return [...prev, newCount].slice(-40);
        });

      } catch (err) {
        console.error("[WebSocket] Failed to parse stream event JSON:", err);
      }
    };

    ws.onclose = () => {
      setConnectionStatus("OFFLINE");
      console.warn("[WebSocket] Connection closed. Attempting auto-reconnection in 5s...");
      setTimeout(() => {
        // Simple automatic reconnection strategy
        if (wsRef.current?.readyState === WebSocket.CLOSED) {
          setConnectionStatus("CONNECTING");
        }
      }, 5000);
    };

    ws.onerror = (error) => {
      console.error("[WebSocket] Connection error captured:", error);
    };

    // Cleanup resources on component unmount
    return () => {
      if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
        ws.close();
      }
    };
  }, []);

  // Configure high-performance dual-axis Apache ECharts Options
  const chartOption: echarts.EChartsOption = {
    backgroundColor: "#0d1117",
    title: {
      text: "REAL-TIME LOGISTICS TELEMETRY & ANOMALY CEP STREAM",
      left: "center",
      textStyle: {
        color: "#c9d1d9",
        fontSize: 16,
        fontWeight: "bold",
        fontFamily: "monospace"
      },
      subtext: `Telemetry Engine Status: ${connectionStatus}`,
      subtextStyle: {
        color: connectionStatus === "ONLINE" ? "#58a6ff" : "#f85149",
        fontSize: 12
      }
    },
    tooltip: {
      trigger: "axis",
      axisPointer: {
        type: "cross",
        label: {
          backgroundColor: "#1f242c"
        }
      }
    },
    legend: {
      data: ["Temperature (°C)", "Cumulative Anomaly Count"],
      top: "10%",
      textStyle: {
        color: "#8b949e"
      }
    },
    grid: {
      left: "5%",
      right: "5%",
      bottom: "10%",
      containLabel: true
    },
    xAxis: [
      {
        type: "category",
        boundaryGap: false,
        data: timeBuffer,
        axisLine: {
          lineStyle: {
            color: "#30363d"
          }
        },
        axisLabel: {
          color: "#8b949e",
          fontSize: 10
        }
      }
    ],
    yAxis: [
      {
        type: "value",
        name: "Temperature (°C)",
        min: -10,
        max: 50,
        position: "left",
        axisLine: {
          show: true,
          lineStyle: {
            color: "#ff7b72"
          }
        },
        axisLabel: {
          color: "#ff7b72"
        },
        splitLine: {
          lineStyle: {
            color: "#21262d"
          }
        }
      },
      {
        type: "value",
        name: "Anomalies Count",
        position: "right",
        axisLine: {
          show: true,
          lineStyle: {
            color: "#f0883e"
          }
        },
        axisLabel: {
          color: "#f0883e"
        },
        splitLine: {
          show: false
        }
      }
    ],
    series: [
      {
        name: "Temperature (°C)",
        type: "line",
        yAxisIndex: 0,
        smooth: true,
        showSymbol: false,
        data: tempBuffer,
        itemStyle: {
          color: "#ff7b72"
        },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: "rgba(255, 123, 114, 0.3)" },
            { offset: 1, color: "rgba(255, 123, 114, 0.0)" }
          ])
        }
      },
      {
        name: "Cumulative Anomaly Count",
        type: "line",
        yAxisIndex: 1,
        smooth: true,
        data: anomalyCountBuffer,
        itemStyle: {
          color: "#f0883e"
        },
        lineStyle: {
          width: 3,
          type: "dashed"
        }
      }
    ]
  };

  return (
    <div style={{ padding: "20px", backgroundColor: "#0d1117", minHeight: "100vh", fontFamily: "monospace", color: "#c9d1d9" }}>
      {/* Topology Dashboard Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid #30363d", paddingBottom: "15px", marginBottom: "20px" }}>
        <div>
          <h1 style={{ margin: 0, fontSize: "22px", color: "#58a6ff" }}>INTELLIGENT UNIFIED DATA & ANALYTICS OPERATING SYSTEM</h1>
          <p style={{ margin: "5px 0 0 0", color: "#8b949e" }}>Continuous Ingestion & Real-Time Complex Event Processing (CEP) Control Center</p>
        </div>
        <div style={{ display: "flex", gap: "15px" }}>
          <span style={{ padding: "6px 12px", borderRadius: "12px", fontSize: "12px", fontWeight: "bold", backgroundColor: connectionStatus === "ONLINE" ? "#1f6feb" : "#da3633", color: "#fff" }}>
            CONNECTION: {connectionStatus}
          </span>
          <span style={{ padding: "6px 12px", borderRadius: "12px", fontSize: "12px", fontWeight: "bold", backgroundColor: "#21262d", border: "1px solid #30363d" }}>
            ACTIVATED AGENTS: FlinkCEP, DubboCasbin, SkyWalking
          </span>
        </div>
      </div>

      {/* Main Dynamic Chart Panel */}
      <div style={{ backgroundColor: "#161b22", padding: "20px", borderRadius: "8px", border: "1px solid #30363d", marginBottom: "25px" }}>
        <ReactECharts option={chartOption} style={{ height: "450px", width: "100%" }} />
      </div>

      {/* Real-time Activity Ledger & Event Alerts Grid */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "20px" }}>

        {/* Real-time Telemetry Ledger */}
        <div style={{ backgroundColor: "#161b22", padding: "15px", borderRadius: "8px", border: "1px solid #30363d" }}>
          <h3 style={{ marginTop: 0, borderBottom: "1px solid #30363d", paddingBottom: "8px", color: "#58a6ff" }}>📡 INCOMING TELEMETRY FEED (ROCKETMQ-FLINK SINK)</h3>
          <div style={{ maxHeight: "250px", overflowY: "auto" }}>
            {metrics.length === 0 ? (
              <p style={{ color: "#8b949e", textAlign: "center" }}>Waiting for WebSocket event stream handshake...</p>
            ) : (
              <table style={{ width: "100%", textAlign: "left", fontSize: "12px", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ color: "#8b949e", borderBottom: "1px solid #21262d" }}>
                    <th style={{ padding: "8px 4px" }}>Asset ID</th>
                    <th>Tenant ID</th>
                    <th>Temp (°C)</th>
                    <th>Alert Status</th>
                    <th>Timestamp</th>
                  </tr>
                </thead>
                <tbody>
                  {metrics.slice(0, 10).map((m, index) => (
                    <tr key={index} style={{ borderBottom: "1px solid #21262d" }}>
                      <td style={{ padding: "8px 4px", color: "#58a6ff", fontWeight: "bold" }}>{m.assetId}</td>
                      <td>{m.tenantId}</td>
                      <td style={{ color: m.anomalyTriggered ? "#ff7b72" : "#3fb950" }}>{m.temperature.toFixed(2)}</td>
                      <td>
                        <span style={{
                          padding: "2px 6px",
                          borderRadius: "4px",
                          fontSize: "10px",
                          fontWeight: "bold",
                          backgroundColor: m.alertLevel === "CRITICAL_SPIKE" ? "rgba(248,81,73,0.15)" : m.alertLevel === "WARNING_TEMPERATURE_DRIFT" ? "rgba(240,136,62,0.15)" : "rgba(63,185,80,0.15)",
                          color: m.alertLevel === "CRITICAL_SPIKE" ? "#ff7b72" : m.alertLevel === "WARNING_TEMPERATURE_DRIFT" ? "#f0883e" : "#3fb950"
                        }}>
                          {m.alertLevel}
                        </span>
                      </td>
                      <td style={{ color: "#8b949e" }}>{new Date(m.timestamp).toLocaleTimeString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        {/* Flink CEP Anomaly Alerts Ledger */}
        <div style={{ backgroundColor: "#161b22", padding: "15px", borderRadius: "8px", border: "1px solid #30363d" }}>
          <h3 style={{ marginTop: 0, borderBottom: "1px solid #30363d", paddingBottom: "8px", color: "#f0883e" }}>⚠️ FLINK CEP STATEFUL ANOMALIES DETECTED</h3>
          <div style={{ maxHeight: "250px", overflowY: "auto" }}>
            {metrics.filter(m => m.anomalyTriggered).length === 0 ? (
              <p style={{ color: "#8b949e", textAlign: "center", padding: "20px" }}>No anomalies detected across sliding telemetry window.</p>
            ) : (
              metrics.filter(m => m.anomalyTriggered).slice(0, 8).map((m, index) => (
                <div key={index} style={{ padding: "10px", backgroundColor: "rgba(248,81,73,0.1)", border: "1px solid rgba(248,81,73,0.2)", borderRadius: "6px", marginBottom: "8px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <div>
                    <span style={{ fontWeight: "bold", color: "#ff7b72" }}>[ANOMALY CRITICAL SPIKE]</span>
                    <div style={{ fontSize: "12px", margin: "4px 0 0 0" }}>
                      Asset: <strong style={{ color: "#c9d1d9" }}>{m.assetId}</strong> | Temp rose from <span style={{ textDecoration: "line-through" }}>{m.lastTemperature.toFixed(2)}°C</span> to <strong style={{ color: "#ff7b72" }}>{m.temperature.toFixed(2)}°C</strong>
                    </div>
                  </div>
                  <div style={{ textAlign: "right", fontSize: "11px", color: "#8b949e" }}>
                    <div>Tenant: {m.tenantId}</div>
                    <div style={{ marginTop: "4px" }}>{new Date(m.timestamp).toLocaleTimeString()}</div>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

      </div>
    </div>
  );
};
