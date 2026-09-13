plugins {
    `java-library`
    alias(libs.plugins.vanniktech.publish)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    api(project(":tiercache-core"))
    // Reactor 3.7.x is the line managed by Spring Boot 3.5 (Reactor 2024.0.x).
    api("io.projectreactor:reactor-core:3.7.19")

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(testFixtures(project(":tiercache-core")))
    testImplementation("io.projectreactor:reactor-test:3.7.19")
    testImplementation("org.assertj:assertj-core:3.27.3")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// --- Publishing (release automation): shared Central Portal target, license,
// and scm metadata come from the root build script.
mavenPublishing {
    pom {
        name.set("tiercache-reactor")
        description.set(
            "Reactor API for the Tiercache two-level cache:" +
                " Mono facade, invalidation Flux"
        )
    }
}
