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
    implementation(testFixtures(project(":tiercache-core")))
    implementation(project(":tiercache-transport-redis"))
    implementation(project(":tiercache-invalidation"))
    implementation(project(":tiercache-micrometer"))
    implementation(libs.micrometer.core)

    implementation(platform(libs.junit.bom))
    implementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    implementation(libs.testcontainers)
    implementation(libs.testcontainers.junit.jupiter)
}

tasks.withType<Test> {
    useJUnitPlatform()
}
