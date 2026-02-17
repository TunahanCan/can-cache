Can-Cache-Agent
Can-Cache-Agent is a Quarkus + Vert.x TCP edge proxy for Can-Cache (memcached-compatible) nodes running on Kubernetes.

Features
L4 TCP proxy on a single external port (11211 by default)
Kubernetes DNS-based upstream discovery (no K8s API / RBAC)
Async health checks with UP/DOWN/UNKNOWN state tracking
Selection policies: RR and LEAST_CONN
Backpressure-aware byte forwarding with pause/resume + drainHandler
Terminal dashboard (ANSI TUI) when running in TTY
Snapshot logging mode for non-TTY environments (pods/log collectors)
Run
mvn quarkus:dev
or package:

mvn package
java -jar target/quarkus-app/quarkus-run.jar
Configuration
See src/main/resources/application.properties.

Important defaults:

agent.listen.host=0.0.0.0
agent.listen.port=11211
agent.discovery.dns=cache-headless.default.svc.cluster.local
agent.discovery.interval=5s
agent.health.interval=2s
agent.health.connect-timeout=1500ms
agent.selection.policy=RR
agent.timeouts.idle=60s
agent.dashboard.mode=auto (auto, tui, log, compact)
compact mode keeps a single snapshot line updated in place (no scrolling) and is useful for attached terminals where full TUI is not desired.

Keyboard commands (TUI mode)
q: quit
r: force DNS refresh
h: help event