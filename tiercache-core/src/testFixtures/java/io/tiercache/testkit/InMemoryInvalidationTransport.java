package io.tiercache.testkit;

import io.tiercache.InvalidationMessage;
import io.tiercache.spi.InvalidationTransport;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * In-memory {@link InvalidationTransport}: dispatch through a shared hub to
 * every currently-connected transport instance, plus a simulated disconnect
 * switch for chaos tests (a disconnected receiver misses messages — the
 * Pub/Sub non-durability model).
 */
public final class InMemoryInvalidationTransport implements InvalidationTransport {

    /** Shared bus: every transport on the same hub sees all messages. */
    public static final class Hub {
        final List<InMemoryInvalidationTransport> members = new CopyOnWriteArrayList<>();
    }

    private final Hub hub;
    private final AtomicBoolean connected = new AtomicBoolean(true);
    private final Map<String, Consumer<InvalidationMessage>> localHandlers = new ConcurrentHashMap<>();
    private Runnable reconnectListener = () -> {
    };

    public InMemoryInvalidationTransport(Hub hub) {
        this.hub = hub;
        hub.members.add(this);
    }

    @Override
    public void publish(InvalidationMessage message) {
        if (!connected.get()) {
            return; // can't publish while disconnected
        }
        for (InMemoryInvalidationTransport member : hub.members) {
            if (member.connected.get()) {
                member.deliver(message);
            }
        }
    }

    private void deliver(InvalidationMessage message) {
        Consumer<InvalidationMessage> handler = localHandlers.get(message.cache());
        if (handler != null) {
            handler.accept(message);
        }
    }

    @Override
    public AutoCloseable subscribe(String cache, Consumer<InvalidationMessage> handler) {
        localHandlers.put(cache, handler);
        return () -> localHandlers.remove(cache);
    }

    @Override
    public void setReconnectListener(Runnable listener) {
        this.reconnectListener = listener;
    }

    /** This instance stops receiving (and sending) messages. */
    public void disconnect() {
        connected.set(false);
    }

    /** Resumes delivery and notifies the engine to replay the journal. */
    public void reconnect() {
        connected.set(true);
        reconnectListener.run();
    }

    @Override
    public void close() {
        localHandlers.clear();
        hub.members.remove(this);
    }
}
