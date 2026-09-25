package io.tiercache.spi;

/** A quiescent delivery interval; release only after current successful recovery. */
@FunctionalInterface
public interface InvalidationDeliveryFence {
    void release();
    InvalidationDeliveryFence NOOP = () -> { };
}
