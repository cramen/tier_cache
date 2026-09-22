import java.util.Properties
import java.time.Duration

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
    api(project(":tiercache-invalidation"))
    api(libs.lettuce.core)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(testFixtures(project(":tiercache-core")))
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
}

val serverProfiles = Properties().apply {
    rootProject.file("compatibility/platforms.properties").inputStream().use { load(it) }
}
tasks.withType<Test> {
    useJUnitPlatform()
    val serverImage = providers.gradleProperty("serverImage").orElse(serverProfiles.getProperty("redis62"))
    inputs.property("serverImage", serverImage)
    systemProperty("tiercache.test.serverImage", serverImage.get())
    systemProperty("tiercache.test.valkeyImage", serverProfiles.getProperty("valkey"))
}

// --- Publishing (release automation): shared Central Portal target, license,
// and scm metadata come from the root build script.
mavenPublishing {
    pom {
        name.set("tiercache-transport-redis")
        description.set(
            "Redis/Valkey transport for the Tiercache two-level cache:" +
                " Lettuce-backed L2, lock provider, Pub/Sub and Streams invalidation profiles"
        )
    }
}

// Run the same real-server contracts for each CI profile, with separate reports.
tasks.register<Test>("serverContractTest") {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    exclude("**/ValkeyLettuceContractTest.class")
    outputs.upToDateWhen { false }
    timeout.set(Duration.ofMinutes(15))
}
