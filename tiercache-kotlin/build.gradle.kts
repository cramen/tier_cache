plugins {
    `java-library`
    kotlin("jvm") version "2.2.21"
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

kotlin {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
    }
}

dependencies {
    api(project(":tiercache-core"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(testFixtures(project(":tiercache-core")))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
    testImplementation("org.assertj:assertj-core:3.27.3")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
