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
    // The GraalVM reachability metadata repository schema requires a newer
    // GraalVM than the JDK 17 line ships; the native compiler runs on a
    // GraalVM 25 toolchain while the sources keep compiling at release 17.
    binaries.named("main") {
        // Without the `application` plugin the main class is not inferred
        // reliably and native-image silently builds a shared library
        // instead of an executable; set it explicitly.
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
