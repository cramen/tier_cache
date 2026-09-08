package io.tiercache.micrometer;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.tiercache.spi.CacheMetricsListener;

/**
 * OpenTelemetry spans for L2 operations and inbound invalidation
 * processing. No spans on the L1-hit path (core never calls the hooks
 * there). Attributes: {@code cache.name}, {@code cache.hit},
 * {@code db.system=redis}.
 */
public final class TiercacheTracing implements CacheMetricsListener {

    private final Tracer tracer;

    public TiercacheTracing(OpenTelemetry openTelemetry) {
        this.tracer = openTelemetry.getTracer("io.tiercache");
    }

    @Override
    public Object onL2OperationStart(String cache, String operation) {
        Span span = tracer.spanBuilder("tiercache.l2." + operation)
                .setAttribute("cache.name", cache)
                .setAttribute("db.system", "redis")
                .startSpan();
        return new Handle(span, span.makeCurrent());
    }

    @Override
    public void onL2OperationEnd(String cache, String operation, boolean hit, Object handle) {
        if (handle instanceof Handle h) {
            h.span.setAttribute("cache.hit", hit);
            h.scope.close();
            h.span.end();
        }
    }

    @Override
    public Object onInvalidationStart(String cache) {
        Span span = tracer.spanBuilder("tiercache.invalidation.apply")
                .setAttribute("cache.name", cache)
                .setAttribute("db.system", "redis")
                .startSpan();
        return new Handle(span, span.makeCurrent());
    }

    @Override
    public void onInvalidationEnd(String cache, Object handle) {
        if (handle instanceof Handle h) {
            h.scope.close();
            h.span.end();
        }
    }

    private record Handle(Span span, Scope scope) {
    }
}
