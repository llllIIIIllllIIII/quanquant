"""Market Pulse v0.1 — price-velocity audio/Telegram alert module.

Detects sudden acceleration in the last-traded price (tick velocity) and maps it
to a Velocity Level 0–4. Pure metric math lives in `metrics`; the streaming
`PulseEngine` (poller subscriber + Telegram edge-notifier) lives in `engine`.
"""
