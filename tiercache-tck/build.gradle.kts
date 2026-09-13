plugins {
    `java-library`
    alias(libs.plugins.jmh)
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

// Benchmark source set for the cascade throughput benchmark (benchmark-suite
// design D1): JMH against a real Redis container. The JMH plugin is applied
// for its task types and the `jmh` dependency bucket (jmh-core + generator),
// but the plugin's own `jmh` source set/task stay unused so this module keeps
// a single benchmark entry point named `jmhBenchmark`.
val benchmark = sourceSets.create("benchmark") {
    java.srcDir("src/benchmark/java")
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

    // JMH generator on the annotation-processor path: generated benchmark
    // classes are compiled with the benchmark sources, no bytecode
    // post-processing tasks needed. Version matches the plugin default.
    "benchmarkAnnotationProcessor"("org.openjdk.jmh:jmh-generator-annprocess:1.36")
}

configurations {
    named("benchmarkImplementation") {
        extendsFrom(configurations.api.get(), configurations.implementation.get(),
            configurations.jmh.get())
    }
    named("benchmarkRuntimeOnly") {
        extendsFrom(configurations.runtimeOnly.get())
    }
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

// Compliance-suite artifact (TCK publication design D5): the TCK's value is
// its Testcontainers chaos suite, which lives in the test source set and is
// not packaged by the default `jar`. Ship it as a dedicated jar with a
// `tests` classifier; the main output is included because the suite uses
// StampedeHarness from it. The `benchmark` and `vtStress` source sets stay
// out of this artifact. No module in this build configures maven-publish, so
// there is no pom metadata to mirror; group/version come from the root
// subprojects block like every other module.
val tckTestsJar = tasks.register<Jar>("tckTestsJar") {
    description = "Compliance-suite classes (chaos TCK) for consumer runs."
    group = "build"
    dependsOn("testClasses")
    from(sourceSets.main.get().output)
    from(sourceSets.test.get().output)
    archiveClassifier.set("tests")
}

tasks.named("assemble") {
    dependsOn(tckTestsJar)
}

// The plugin's own `jmh` task would run an empty fork: this module's JMH
// entry point is `jmhBenchmark` (below) over the `benchmark` source set.
tasks.named("jmh") {
    enabled = false
}

val jmhBenchmarkJar = tasks.register<Jar>("jmhBenchmarkJar") {
    description = "Benchmark classes for the cascade throughput benchmark."
    group = "jmh"
    dependsOn("benchmarkClasses")
    from(benchmark.output)
    from(sourceSets.main.get().output)
    archiveClassifier.set("jmh-benchmark")
}

tasks.register<me.champeau.jmh.JMHTask>("jmhBenchmark") {
    description = "Cascade-read throughput benchmark (L1 miss -> L2 hit -> L1 warm) against a Redis container."
    group = "jmh"
    dependsOn(jmhBenchmarkJar)
    jmhClasspath.from(configurations.jmh.get())
    testRuntimeClasspath.from(benchmark.runtimeClasspath)
    jarArchive.set(jmhBenchmarkJar.flatMap { it.archiveFile })
    resultsFile.set(layout.buildDirectory.file("results/jmh-benchmark/results.txt"))
    fork.set(providers.gradleProperty("jmh.fork").map(String::toInt).orElse(1))
    warmupIterations.set(providers.gradleProperty("jmh.warmupIterations").map(String::toInt).orElse(3))
    iterations.set(providers.gradleProperty("jmh.iterations").map(String::toInt).orElse(5))
    javaLauncher.set(javaToolchains.launcherFor(java.toolchain))
}

tasks.register<JavaExec>("propagationBenchmark") {
    description = "Invalidation propagation latency harness (Pub/Sub profile): p50/p95/p99 publish-to-applied."
    group = "verification"
    mainClass.set("io.tiercache.tck.PropagationBenchmark")
    classpath = sourceSets.test.get().runtimeClasspath
    systemProperty("tiercache.propagation.events",
        providers.systemProperty("tiercache.propagation.events").orElse("10000").get())
    systemProperty("tiercache.propagation.report",
        layout.buildDirectory.dir("results/propagation").get().file("results.txt").asFile.absolutePath)
}
