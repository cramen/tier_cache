# Tiercache Spring demo

The 15-minute quick start:

1. Start Redis: `docker compose up -d` (in this directory)
2. Run the app: `./gradlew :examples:demo-spring:bootRun` (or run `DemoApplication` from your IDE)
3. `curl localhost:8080/greeting/world` — first call takes ~500 ms (simulated expensive computation)
4. Repeat — served from L1 in microseconds; restart the app and repeat — served from L2 (Redis)

Configuration used (application.yml): `tiercache.enabled=true` + `tiercache.redis-uri`. That's the whole setup.

## Cache endpoints

`PUT /cache/{key}` (text body) stores a value, `GET /cache/{key}` reads it (404 on miss), `DELETE /cache/{key}` evicts it.

## Native Image

The app builds to a native binary via the GraalVM Native Build Tools: `./gradlew :examples:demo-spring:nativeCompile` (requires a GraalVM 25 toolchain). The `native-smoke` CI job compiles it and runs the cache endpoints against a real Redis. Note: the GraalVM reachability metadata repository is intentionally disabled for this build — its Netty 4.1 entries would override the version-matched metadata shipped inside the Netty 4.2 jars.
