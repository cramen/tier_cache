plugins {
    `java-library`
    // Version comes from the root plugins block (shared classloader with the
    // publishing plugin).
    kotlin("jvm")
    alias(libs.plugins.vanniktech.publish)
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

// --- Publishing (release automation): shared Central Portal target, license,
// and scm metadata come from the root build script.
mavenPublishing {
    pom {
        name.set("tiercache-kotlin")
        description.set(
            "Kotlin coroutines API for the Tiercache two-level cache:" +
                " suspend facade, invalidation Flow, config DSL"
        )
    }
}
