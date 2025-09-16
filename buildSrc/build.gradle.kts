plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

dependencies {
    implementation("com.github.johnrengelman:shadow:8.1.1")
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:1.9.20")
}