package io.tiercache.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
