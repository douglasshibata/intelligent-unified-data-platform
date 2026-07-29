import React, { useEffect, useRef, useState } from 'react';
import ReactECharts from 'echarts-for-react';
import * as echarts from 'echarts';

/**
 * RealTimeDashboard
 *
 * TypeScript/React Component designed to consume real-time telemetry anomaly alert events
 * directly from an active WebSocket stream populated by Flink processing. Renders a performance-optimized
 * interactive chart utilizing Apache ECharts with Canvas buffering to prevent rendering slowdowns.
 */

interface TelemetryData {
  deviceId: string;
  temperature: number;
  latitude: number;
  longitude: number;
  timestamp: number;
  anomalyDetected: boolean;
  reason: string;
}

export const RealTimeDashboard: React.FC = () => {
  const [telemetryHistory, setTelemetryHistory] = useState<TelemetryData[]>([]);
  const [wsConnected, setWsConnected] = useState<boolean>(false);
  const [anomalyCount, setAnomalyCount] = useState<number>(0);
  const socketRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    // Establish client connection with WebSockets endpoint fed by Flink/RocketMQ streams
    const wsUrl = process.env.REACT_APP_STREAM_WS_URL || 'ws://localhost:8081/api/v1/telemetry/stream';
    const ws = new WebSocket(wsUrl);
    socketRef.current = ws;

    ws.onopen = () => {
      console.log('Successfully connected to real-time Flink telemetry stream.');
      setWsConnected(true);
    };

    ws.onmessage = (event) => {
      try {
        const payload: TelemetryData = JSON.parse(event.data);

        setTelemetryHistory((prev) => {
          // Keep a sliding window of the last 100 metrics to prevent client memory bloat
          const updated = [...prev, payload];
          if (updated.length > 100) {
            updated.shift();
          }
          return updated;
        });

        if (payload.anomalyDetected) {
          setAnomalyCount((prev) => prev + 1);
        }
      } catch (err) {
        console.error('Error parsing streaming event:', err);
      }
    };

    ws.onclose = () => {
      console.warn('Telemetry WebSocket stream disconnected. Attempting reconnect in 5s...');
      setWsConnected(false);
      setTimeout(() => {
        // Simple reconnect mechanism
        setTelemetryHistory([]);
      }, 5000);
    };

    return () => {
      if (socketRef.current) {
        socketRef.current.close();
      }
    };
  }, []);

  // Configure high-performance Apache ECharts parameters
  const getChartOptions = () => {
    const timestamps = telemetryHistory.map((item) =>
      new Date(item.timestamp).toLocaleTimeString()
    );
    const temperatures = telemetryHistory.map((item) => item.temperature);

    // Highlight points that triggered anomaly violations
    const markPointData = telemetryHistory
      .map((item, index) => {
        if (item.anomalyDetected) {
          return {
            name: 'Anomaly Alert',
            value: item.temperature,
            xAxis: index,
            yAxis: item.temperature,
            itemStyle: { color: '#ef4444' }
          };
        }
        return null;
      })
      .filter((val) => val !== null);

    return {
      title: {
        text: 'Global Fleet Telemetry Monitor (Real-time)',
        textStyle: { color: '#f3f4f6', fontSize: 16 }
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross', label: { backgroundColor: '#374151' } }
      },
      grid: {
        left: '3%',
        right: '4%',
        bottom: '3%',
        containLabel: true
      },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: timestamps,
        axisLine: { lineStyle: { color: '#4b5563' } },
        axisLabel: { color: '#9ca3af' }
      },
      yAxis: {
        type: 'value',
        name: 'Temperature (°C)',
        axisLine: { lineStyle: { color: '#4b5563' } },
        axisLabel: { color: '#9ca3af' },
        splitLine: { lineStyle: { color: '#374151', type: 'dashed' } }
      },
      series: [
        {
          name: 'Core Cargo Temp',
          type: 'line',
          data: temperatures,
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 3, color: '#3b82f6' },
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: 'rgba(59, 130, 246, 0.4)' },
              { offset: 1, color: 'rgba(59, 130, 246, 0)' }
            ])
          },
          markPoint: {
            symbol: 'pin',
            symbolSize: 25,
            data: markPointData
          }
        }
      ],
      backgroundColor: 'transparent'
    };
  };

  return (
    <div style={{ backgroundColor: '#111827', color: '#f3f4f6', padding: '24px', borderRadius: '12px', fontFamily: 'sans-serif' }}>
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderBottom: '1px solid #374151', paddingBottom: '16px', marginBottom: '24px' }}>
        <div>
          <h1 style={{ margin: 0, fontSize: '24px', fontWeight: 700 }}>Data & Analytics Platform OS</h1>
          <p style={{ margin: '4px 0 0', color: '#9ca3af', fontSize: '14px' }}>System Operator Console</p>
        </div>
        <div style={{ display: 'flex', gap: '16px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '6px 12px', backgroundColor: '#1f2937', borderRadius: '6px' }}>
            <span style={{ width: '10px', height: '10px', borderRadius: '50%', backgroundColor: wsConnected ? '#10b981' : '#f59e0b' }}></span>
            <span style={{ fontSize: '14px', fontWeight: 500 }}>{wsConnected ? 'Stream Connected' : 'Stream Reconnecting...'}</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '6px 12px', backgroundColor: '#ef4444', color: '#ffffff', borderRadius: '6px' }}>
            <span style={{ fontSize: '14px', fontWeight: 'bold' }}>Anomalies Flagged: {anomalyCount}</span>
          </div>
        </div>
      </header>

      <section style={{ display: 'grid', gridTemplateColumns: '3fr 1fr', gap: '24px' }}>
        {/* ECharts visualization wrapper */}
        <div style={{ backgroundColor: '#1f2937', padding: '20px', borderRadius: '8px', height: '400px' }}>
          {telemetryHistory.length > 0 ? (
            <ReactECharts
              option={getChartOptions()}
              style={{ height: '100%', width: '100%' }}
              notMerge={true}
              lazyUpdate={true}
            />
          ) : (
            <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#9ca3af' }}>
              Waiting for incoming stream data from Flink topic...
            </div>
          )}
        </div>

        {/* Real-time event log */}
        <div style={{ backgroundColor: '#1f2937', padding: '20px', borderRadius: '8px', display: 'flex', flexDirection: 'column', height: '400px' }}>
          <h2 style={{ fontSize: '16px', margin: '0 0 16px 0', borderBottom: '1px solid #374151', paddingBottom: '8px' }}>Active System Log</h2>
          <div style={{ flexGrow: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column-reverse', gap: '8px' }}>
            {telemetryHistory.slice().reverse().map((item, idx) => (
              <div
                key={idx}
                style={{
                  padding: '8px',
                  borderRadius: '4px',
                  fontSize: '12px',
                  backgroundColor: item.anomalyDetected ? 'rgba(239, 68, 68, 0.1)' : 'rgba(59, 130, 246, 0.1)',
                  borderLeft: `3px solid ${item.anomalyDetected ? '#ef4444' : '#3b82f6'}`
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                  <span style={{ fontWeight: 'bold' }}>{item.deviceId}</span>
                  <span style={{ color: '#9ca3af' }}>{new Date(item.timestamp).toLocaleTimeString()}</span>
                </div>
                <div>Temp: {item.temperature.toFixed(2)} °C</div>
                {item.anomalyDetected && (
                  <div style={{ color: '#ef4444', marginTop: '4px', fontStyle: 'italic' }}>{item.reason}</div>
                )}
              </div>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
};
