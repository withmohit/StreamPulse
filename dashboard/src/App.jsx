import { useState, useEffect, useCallback } from 'react'

const API           = '/api'
const REFRESH_MS    = 10_000

// ─── data hook ──────────────────────────────────────────────────────────────
function useMetrics() {
  const [data,      setData]      = useState(null)
  const [updatedAt, setUpdatedAt] = useState(null)
  const [loading,   setLoading]   = useState(true)
  const [err,       setErr]       = useState(null)

  const refresh = useCallback(async () => {
    try {
      const [health, throughput, lag, errors] = await Promise.all([
        fetch(`${API}/health`).then(r    => { if (!r.ok) throw new Error(r.status); return r.json() }),
        fetch(`${API}/throughput`).then(r => { if (!r.ok) throw new Error(r.status); return r.json() }),
        fetch(`${API}/lag`).then(r       => { if (!r.ok) throw new Error(r.status); return r.json() }),
        fetch(`${API}/errors`).then(r    => { if (!r.ok) throw new Error(r.status); return r.json() }),
      ])
      setData({ health, throughput, lag, errors })
      setUpdatedAt(new Date().toLocaleTimeString())
      setErr(null)
    } catch (e) {
      setErr('Metrics service unreachable')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    refresh()
    const t = setInterval(refresh, REFRESH_MS)
    return () => clearInterval(t)
  }, [refresh])

  return { data, updatedAt, loading, err, refresh }
}

// ─── shared primitives ──────────────────────────────────────────────────────
function Dot({ ok }) {
  return <span className={`dot ${ok ? 'dot-ok' : 'dot-err'}`}>●</span>
}

function Card({ title, children }) {
  return (
    <div className="card">
      <p className="card-label">{title}</p>
      {children}
    </div>
  )
}

function BigNum({ value, label }) {
  return (
    <div className="bignum">
      <span className="bignum-value">{value ?? '—'}</span>
      <span className="bignum-label">{label}</span>
    </div>
  )
}

function Divider() {
  return <div className="divider" />
}

// ─── Health card ────────────────────────────────────────────────────────────
function HealthCard({ health }) {
  const rows = [
    { key: 'postgres', label: 'Postgres' },
    { key: 'redis',    label: 'Redis'    },
  ]
  return (
    <Card title="Health">
      <BigNum value={health.status.toUpperCase()} label="overall" />
      <Divider />
      {rows.map(({ key, label }) => (
        <div key={key} className="dep-row">
          <Dot ok={health[key] === 'connected'} />
          <span>{label}</span>
          <span className="dep-val">{health[key]}</span>
        </div>
      ))}
    </Card>
  )
}

// ─── Throughput card ─────────────────────────────────────────────────────────
function ThroughputCard({ throughput }) {
  const { total_events, last_window } = throughput
  return (
    <Card title="Throughput">
      <BigNum value={total_events?.toLocaleString()} label="total events" />
      {last_window && (
        <>
          <Divider />
          <p className="sub-label">Last 60 s window · {last_window.total} events</p>
          <div className="type-grid">
            {Object.entries(last_window.by_type).map(([type, count]) => (
              <div key={type} className="type-cell">
                <span className="type-count">{count}</span>
                <span className="type-name">{type}</span>
              </div>
            ))}
          </div>
        </>
      )}
      {!last_window && <p className="muted">No window data yet</p>}
    </Card>
  )
}

// ─── Lag card ────────────────────────────────────────────────────────────────
function LagCard({ lag }) {
  const { topic, group_id, partitions = {}, total_lag } = lag
  const maxLag = Math.max(1, ...Object.values(partitions))

  return (
    <Card title="Consumer Lag">
      <BigNum value={total_lag} label="total lag" />
      <Divider />
      <p className="sub-label">{topic} · {group_id}</p>
      {Object.entries(partitions).map(([p, l]) => (
        <div key={p} className="lag-row">
          <span className="lag-p">P{p}</span>
          <div className="lag-track">
            <div
              className="lag-fill"
              style={{
                width: `${Math.max(2, (l / maxLag) * 100)}%`,
                background: l > 1000 ? 'var(--red)' : l > 100 ? 'var(--yellow)' : 'var(--green)',
              }}
            />
          </div>
          <span className="lag-num">{l}</span>
        </div>
      ))}
      {Object.keys(partitions).length === 0 && (
        <p className="muted">No lag data yet — consumer may not have run</p>
      )}
    </Card>
  )
}

// ─── Errors card ─────────────────────────────────────────────────────────────
function ErrorsCard({ errors }) {
  const { dlq_total, top_error_reasons = [] } = errors
  return (
    <Card title="DLQ / Errors">
      <BigNum value={dlq_total?.toLocaleString()} label="dead-letter total" />
      {top_error_reasons.length > 0 && (
        <>
          <Divider />
          <p className="sub-label">Top reasons</p>
          {top_error_reasons.map(({ error_reason, count }) => (
            <div key={error_reason} className="reason-row">
              <span className="reason-name">{error_reason}</span>
              <span className="reason-count">{count}</span>
            </div>
          ))}
        </>
      )}
      {top_error_reasons.length === 0 && <p className="muted">No errors in DLQ</p>}
    </Card>
  )
}

// ─── App ─────────────────────────────────────────────────────────────────────
export default function App() {
  const { data, updatedAt, loading, err, refresh } = useMetrics()

  return (
    <div className="shell">
      <header className="header">
        <span className="logo">StreamPulse</span>
        <div className="header-right">
          {err
            ? <span className="badge badge-err">Offline</span>
            : <span className="badge badge-ok">Live</span>}
          {updatedAt && <span className="updated">Updated {updatedAt}</span>}
          <button className="btn-refresh" onClick={refresh} title="Refresh now">↺</button>
        </div>
      </header>

      <main className="grid">
        {loading && !data && (
          <div className="loading">Connecting to metrics service…</div>
        )}

        {err && (
          <div className="error-banner">
            {err} — is <code>uvicorn metrics.main:app --port 8001</code> running?
          </div>
        )}

        {data && (
          <>
            <HealthCard     health={data.health}         />
            <ThroughputCard throughput={data.throughput} />
            <LagCard        lag={data.lag}               />
            <ErrorsCard     errors={data.errors}         />
          </>
        )}
      </main>
    </div>
  )
}
