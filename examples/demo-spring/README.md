# Tiercache Spring demo

The 15-minute quick start:

1. Start Redis: `docker compose up -d` (in this directory)
2. Run the app: `./gradlew :examples:demo-spring:bootRun` (or run `DemoApplication` from your IDE)
3. `curl localhost:8080/greeting/world` — first call takes ~500 ms (simulated expensive computation)
4. Repeat — served from L1 in microseconds; restart the app and repeat — served from L2 (Redis)

Configuration used (application.yml): `tiercache.enabled=true` + `tiercache.redis-uri`. That's the whole setup.
