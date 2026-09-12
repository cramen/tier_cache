plugins {
    `java-library`
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
    // Adds nativeCompile/nativeRun only; `build`/`check` are unaffected.
    // The main class is inherited from the Spring Boot plugin.
    alias(libs.plugins.graalvm.native)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

graalvmNative {
    // The reachability metadata repository must stay disabled here: its
    // netty entries are marked override=true and target the 4.1 line, which
    // silently replaces the version-matched metadata shipped inside the
    // Netty 4.2 jars (Lettuce 7 requires Netty 4.2) and breaks the native
    // build at runtime. In-jar metadata (Netty, Lettuce, this project) is
    // the correct source for this application.
    metadataRepository {
        enabled.set(false)
    }
    // The GraalVM reachability metadata repository schema requires a newer
    // GraalVM than the JDK 17 line ships; the native compiler runs on a
    // GraalVM 25 toolchain while the sources keep compiling at release 17.
    binaries.named("main") {
        // Without the `application` plugin the Native Build Tools default
        // `sharedLibrary` to true, silently producing demo-spring.so instead
        // of an executable; force the executable and set the main class
        // explicitly.
        sharedLibrary.set(false)
        mainClass.set("io.tiercache.demo.DemoApplication")
        javaLauncher.set(javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(25))
        })
    }
}

dependencies {
    implementation(project(":tiercache-spring-boot-starter"))
    implementation("org.springframework.boot:spring-boot-starter-web")
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Demo is not part of `check`; it has its own verification task.
tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("-parameters")
}
