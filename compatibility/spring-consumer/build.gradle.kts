import java.time.Duration

plugins { java }
val bootVersion = providers.gradleProperty("bootVersion").get()
val tiercacheVersion = providers.gradleProperty("tiercacheVersion").get()
val artifactRepository = providers.gradleProperty("artifactRepository").get()
repositories {
    exclusiveContent {
        forRepository { maven { url = uri(artifactRepository) } }
        filter { includeGroup("io.github.cramen") }
    }
    mavenCentral()
}
java { toolchain { languageVersion = JavaLanguageVersion.of(17) } }
dependencies {
    testImplementation(enforcedPlatform("org.springframework.boot:spring-boot-dependencies:$bootVersion"))
    testImplementation("io.github.cramen:tiercache-spring-boot-starter:$tiercacheVersion")
    testImplementation("io.github.cramen:tiercache-micrometer:$tiercacheVersion")
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
tasks.withType<JavaCompile> { options.compilerArgs.add("-parameters") }
tasks.test {
    useJUnitPlatform()
    timeout.set(Duration.ofMinutes(10))
    systemProperty("tiercache.test.redisUri", providers.gradleProperty("redisUri").get())
    systemProperty("tiercache.test.bootVersion", bootVersion)
    inputs.property("bootVersion", bootVersion)
    outputs.upToDateWhen { false }
}
tasks.register("dependencyEvidence") {
    doLast {
        val artifacts = configurations.testRuntimeClasspath.get().resolvedConfiguration.resolvedArtifacts
        val report = layout.buildDirectory.file("reports/runtime-dependencies.txt").get().asFile
        report.parentFile.mkdirs()
        report.writeText("Boot BOM: $bootVersion\nTierCache: $tiercacheVersion\nGradle JVM: ${System.getProperty("java.version")}\n" +
            artifacts.sortedBy { it.moduleVersion.id.toString() }.joinToString("\n") {
                "${it.moduleVersion.id} classifier=${it.classifier ?: "main"} file=${it.file.name}"
            } + "\n")
    }
}
tasks.test { dependsOn("dependencyEvidence") }
