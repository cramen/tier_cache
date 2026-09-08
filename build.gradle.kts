plugins {
    // Root project only aggregates; module-specific config lives in each module.
}

subprojects {
    group = "io.tiercache"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}
