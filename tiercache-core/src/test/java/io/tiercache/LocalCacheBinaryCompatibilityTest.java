package io.tiercache;

import io.tiercache.spi.LocalCache;
import io.tiercache.spi.LocalFreshnessResult;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import javax.tools.ToolProvider;
import java.nio.file.*;
import java.net.URLClassLoader;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

class LocalCacheBinaryCompatibilityTest {
    @TempDir Path directory;

    @Test @SuppressWarnings("unchecked")
    void providerCompiledAgainstThePreviousInterfaceLinksAndUsesFallback() throws Exception {
        Path old = directory.resolve("old"), providers = directory.resolve("providers");
        Files.createDirectories(old); Files.createDirectories(providers);
        String frozen;
        try (var in = getClass().getResourceAsStream("/compatibility/LocalCache-v2.java.txt")) {
            assertNotNull(in); frozen = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
        assertFalse(frozen.contains("supportsAtomicFreshnessRead"));
        Path api = directory.resolve("LocalCache.java"); Files.writeString(api, frozen);
        String currentApi = Path.of(LocalCache.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
        var compiler = ToolProvider.getSystemJavaCompiler(); assertNotNull(compiler);
        assertEquals(0, compiler.run(null, null, null, "-proc:none", "--release", "17", "-cp", currentApi,
                "-d", old.toString(), api.toString()));
        Path source = directory.resolve("LegacyProvider.java");
        Files.writeString(source, """
                package compatibility;
                import io.tiercache.spi.*;
                import java.time.Duration;
                import java.util.concurrent.ConcurrentHashMap;
                public class LegacyProvider implements LocalCache<String,String> {
                    private final ConcurrentHashMap<String,StoredEntry<String>> map = new ConcurrentHashMap<>();
                    public int replacements;
                    public StoredEntry<String> get(String k) { return map.get(k); }
                    public void put(String k,StoredEntry<String> v,Duration t) { map.put(k,v); }
                    public void evict(String k) { map.remove(k); }
                    public void clear() { map.clear(); }
                    public boolean setIfAbsent(String k,StoredEntry<String> v,Duration t) { return map.putIfAbsent(k,v)==null; }
                    public boolean supportsAtomicReplace() { return true; }
                    public boolean replaceIfSame(String k,StoredEntry<String> a,StoredEntry<String> b,Duration t) {
                        replacements++; return map.replace(k,a,b);
                    }
                }
                """);
        assertEquals(0, compiler.run(null, null, null, "-proc:none", "--release", "17", "-cp",
                old + java.io.File.pathSeparator + currentApi, "-d", providers.toString(), source.toString()));
        // Parent-first resolution supplies the NEW interface; only old provider bytecode is loaded below.
        try (var loader = new URLClassLoader(new java.net.URL[]{providers.toUri().toURL()}, LocalCache.class.getClassLoader())) {
            var type = loader.loadClass("compatibility.LegacyProvider");
            var local = (LocalCache<String, String>) type.getConstructor().newInstance();
            assertFalse(local.supportsAtomicFreshnessRead());
            assertThrows(UnsupportedOperationException.class,
                    () -> local.readFreshness("x", Duration.ofMinutes(1), Duration.ofMinutes(1)));
            var d = CacheSettings.defaults();
            var settings = new CacheSettings(100, Duration.ofMinutes(5), Duration.ofMinutes(1),
                    Duration.ofHours(1), 0, d.nullPolicy(), d.invalidationMode(), d.payloadCapBytes(),
                    Duration.ZERO, false, d.xfetchBeta(), Duration.ofMinutes(1));
            try (var factory = TierCacheFactory.builder().defaults(settings)
                    .localCacheFactory((n, s) -> local).remoteCache(new InMemoryRemoteCache<String, String>()).build()) {
                TierCache<String, String> cache = factory.getCache("legacy");
                cache.put("x", "value"); assertEquals("value", cache.get("x"));
                assertTrue(type.getField("replacements").getInt(local) > 0);
            }
        }
    }

    @Test void coherentResultRejectsFalseFreshAbsence() {
        assertEquals(LocalFreshnessResult.State.EXPIRED, LocalFreshnessResult.absent().state());
        var value = io.tiercache.spi.StoredEntry.ofValue("v");
        var snapshot = LocalFreshnessResult.of(value, LocalFreshnessResult.State.FRESH);
        assertSame(value, snapshot.entry()); assertEquals(LocalFreshnessResult.State.FRESH, snapshot.state());
        assertNull(LocalFreshnessResult.absent().entry());
        assertThrows(IllegalArgumentException.class, () -> LocalFreshnessResult.of(null, LocalFreshnessResult.State.FRESH));
        assertThrows(NullPointerException.class, () -> LocalFreshnessResult.of(null, null));
    }
}
