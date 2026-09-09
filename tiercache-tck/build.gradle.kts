plugins {
    `java-library`
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

// JDK 21+ source set for the virtual-thread stress gate (design D1): virtual
// threads and the jdk.VirtualThreadPinned JFR event do not exist on the
// Java 17 baseline toolchain.
val vtStress = sourceSets.create("vtStress") {
    java.srcDir("src/vtStress/java")
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

    "vtStressImplementation"(project(":tiercache-core"))
    "vtStressImplementation"(testFixtures(project(":tiercache-core")))
    "vtStressImplementation"(platform(libs.junit.bom))
    "vtStressImplementation"(libs.junit.jupiter)
    "vtStressRuntimeOnly"(libs.junit.platform.launcher)
}

// JDK 21+ toolchain for the virtual-thread stress gate. Resolution is lazy:
// when no 21+ JDK is installed the compile/test tasks below are skipped with
// a loud log line instead of failing (or downloading a JDK).
val vtCompiler = javaToolchains.compilerFor {
    languageVersion = JavaLanguageVersion.of(21)
}
val vtLauncher = javaToolchains.launcherFor {
    languageVersion = JavaLanguageVersion.of(21)
}

fun vtToolchainAvailable(): Boolean = try {
    vtLauncher.get()
    true
} catch (e: Exception) {
    false
}

fun Task.skipUnlessVtToolchain() {
    onlyIf("the virtual-thread stress gate requires a JDK 21+ toolchain") {
        val available = vtToolchainAvailable()
        if (!available) {
            logger.lifecycle("$path SKIPPED: the virtual-thread stress gate requires a JDK 21+ toolchain, none found")
        }
        available
    }
}

tasks.named<JavaCompile>("compileVtStressJava") {
    javaCompiler = vtCompiler
    options.release.set(21)
    skipUnlessVtToolchain()
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("soak")
    }
}

tasks.register<Test>("vtStressTest") {
    description = "Virtual-thread stress gate (JDK 21+): zero carrier pinning on library read paths."
    group = "verification"
    testClassesDirs = vtStress.output.classesDirs
    classpath = vtStress.runtimeClasspath
    javaLauncher = vtLauncher
    skipUnlessVtToolchain()
}

tasks.register<Test>("soakTest") {
    description = "Soak gate: sustained churn against a real L2 container; memory and journal growth gates."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform {
        includeTags("soak")
    }
    systemProperty("tiercache.soak.duration",
        providers.systemProperty("tiercache.soak.duration").orElse("PT10M").get())
}
