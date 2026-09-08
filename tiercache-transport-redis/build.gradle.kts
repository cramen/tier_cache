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
    api(libs.lettuce.core)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(testFixtures(project(":tiercache-core")))
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
}

tasks.withType<Test> {
    useJUnitPlatform()
}
