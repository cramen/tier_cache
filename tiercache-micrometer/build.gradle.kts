plugins {
    `java-library`
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    api(project(":tiercache-core"))
    api(libs.micrometer.core)
    api(libs.otel.api)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(testFixtures(project(":tiercache-core")))
    testImplementation(libs.otel.sdk.testing)
    testImplementation(libs.otel.sdk)
}

tasks.withType<Test> {
    useJUnitPlatform()
}
