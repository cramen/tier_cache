package io.tiercache.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @RestController
    static class GreetingController {

        private final GreetingService service;

        GreetingController(GreetingService service) {
            this.service = service;
        }

        @GetMapping("/greeting/{name}")
        String greeting(@PathVariable String name) {
            return service.greeting(name);
        }
    }

    /**
     * Direct write/read/evict through the Spring Cache surface. Serves the
     * native smoke suite (and manual experimentation): PUT a value, GET it
     * back (repeated GETs hit L1), DELETE to evict.
     */
    @RestController
    static class CacheController {

        private final Cache cache;

        CacheController(CacheManager cacheManager) {
            this.cache = cacheManager.getCache("demo");
        }

        @PutMapping("/cache/{key}")
        ResponseEntity<Void> put(@PathVariable String key, @RequestBody String value) {
            cache.put(key, value);
            return ResponseEntity.noContent().build();
        }

        @GetMapping("/cache/{key}")
        ResponseEntity<String> get(@PathVariable String key) {
            String value = cache.get(key, String.class);
            return value != null ? ResponseEntity.ok(value) : ResponseEntity.notFound().build();
        }

        @DeleteMapping("/cache/{key}")
        ResponseEntity<Void> evict(@PathVariable String key) {
            cache.evict(key);
            return ResponseEntity.noContent().build();
        }
    }

    @org.springframework.stereotype.Service
    static class GreetingService {

        /**
         * Simulates an expensive computation; the second call for the same
         * name is served from the two-level cache.
         */
        @Cacheable("greetings")
        public String greeting(String name) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "Hello, " + name + "!";
        }
    }
}
