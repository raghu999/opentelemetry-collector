# Adaptive Concurrency Extension

The **Adaptive Concurrency Extension** provides a dynamic backpressure mechanism for the OpenTelemetry Collector. It implements an **Adaptive Request Concurrency (ARC)** algorithm that automatically adjusts the number of parallel requests sent to a backend based on real-time performance signals.

## Overview

Traditionally, Collector exporters use a static `num_consumers` setting to control concurrency. However, network conditions and backend capacity are volatile.

* **Static limits set too high** can overwhelm downstreams, leading to latency spikes, HTTP 429s, and system instability.
* **Static limits set too low** underutilize available bandwidth and cause the Collector's internal queues to fill up unnecessarily.

This extension solves these problems by finding the "sweet spot" of maximum throughput automatically using an **AIMD (Additive Increase / Multiplicative Decrease)** control law combined with **EWMA (Exponentially Weighted Moving Average)** latency tracking.

## Technical Details

### The Control Law

The extension monitors the **Round Trip Time (RTT)** and result of every request to adjust the concurrency limit:

1. **Additive Increase (AI):** If requests are succeeding and the latency is within a healthy baseline, the concurrency limit is increased by **1** at the end of every measurement period.
2. **Multiplicative Decrease (MD):** If backpressure is detected, the limit is immediately reduced by a configurable percentage (e.g., cut by 30%).

### Backpressure Triggers

The extension reduces concurrency when either of the following occurs:

* **Error Classification:** The backend returns a "retryable" error (such as HTTP 429 Too Many Requests, HTTP 503 Service Unavailable, or gRPC `ResourceExhausted`).
* **Latency Spikes:** The RTT of recent requests exceeds a calculated threshold:  
  `Threshold = Baseline_Mean + (Deviation_Scale * Baseline_Deviation)`

### Implementation

* **Exporter Integration:** Plugs into the `exporterhelper` via the `RequestMiddleware` interface. It gates the `sending_queue` consumers.
* **Middleware Support:** Can also be used as an HTTP/gRPC server-side interceptor to protect the Collector's ingress.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `enabled` | `true` | Enables the concurrency control logic. |
| `min_concurrency` | `2` | The minimum concurrency limit (floor). |
| `max_concurrency` | `200` | The maximum allowed parallel requests. |
| `decrease_ratio` | `0.7` | The multiplicative factor used during a decrease (e.g., 0.7 = 30% reduction). |
| `ewma_alpha` | `0.05` | The smoothing factor for EWMA (lower = more stable, higher = more reactive). |
| `deviation_scale` | `2.0` | Number of standard deviations above mean RTT to trigger a decrease. |
| `middleware.enabled` | `false` | Enables server-side middleware (ingress protection). |

### Example: Exporter Usage

To use this extension, you must configure it in the `extensions` section and then reference it in the `sending_queue` of your exporter using the `request_middlewares` field.

> **Note:** You should set `num_consumers` in the exporter high enough (e.g., matching `max_concurrency`) so that the worker pool does not artificially cap the adaptive limit.

```yaml
extensions:
  adaptive_concurrency/gradient:
    min_concurrency: 5
    max_concurrency: 100
    decrease_ratio: 0.5

exporters:
  otlp:
    endpoint: https://my-backend:4317
    sending_queue:
      enabled: true
      num_consumers: 100 
      # Reference the extension ID here
      request_middlewares: [adaptive_concurrency/gradient]

service:
  extensions: [adaptive_concurrency/gradient]
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp]


```

---

## Telemetry

The extension provides detailed metrics to observe the controller's behavior.

### Attributes

All metrics are tagged with the following attributes to uniquely identify the throttled component:

* `request_middleware`: The ID of the extension instance.
* `controlled_component`: The ID of the exporter or server being managed.
* `signal`: The data type being exported (e.g., `traces`, `metrics`).

### Metrics

| Metric | Unit | Description |
| --- | --- | --- |
| `adaptive_concurrency.limit` | `1` | The current concurrency limit calculated by the controller. |
| `adaptive_concurrency.permits_in_use` | `1` | Number of requests currently active. |
| `adaptive_concurrency.rtt` | `ms` | Histogram of Round Trip Times for requests. |
| `adaptive_concurrency.backoff_events` | `1` | Count of times the limit was reduced due to backpressure. |
| `adaptive_concurrency.limit_changes` | `1` | Count of total limit adjustments (up or down). |

---

## Operational Guidance

### Interaction with `num_consumers`

The `adaptive_concurrency` extension acts as a gatekeeper *on top* of the existing worker pool.

* The effective concurrency is: `min(Static_Consumers, Controller_Limit)`.
* **Best Practice:** Set `num_consumers` in your exporter to the absolute maximum concurrency your infrastructure can handle, and let the `adaptive_concurrency` extension manage the optimal runtime limit within that ceiling.

### Tuning the Algorithm

* **Stable but Slow Increase:** If the limit is rising too slowly, decrease the `ewma_alpha`.
* **Too Reactive to Jitter:** If the limit drops frequently on a stable network, increase the `deviation_scale` or decrease the `ewma_alpha`.
* **Aggressive Recovery:** To recover faster from an outage, increase the measurement period (internal setting) or set a higher `initial_limit`.

