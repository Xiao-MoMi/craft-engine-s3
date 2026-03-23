plugins {
    id("java")
    id("maven-publish")
    id("com.gradleup.shadow") version "9.2.2"
}

group = "net.momirealms"
version = "0.16"

repositories {
    mavenCentral()
    maven("https://repo.momirealms.net/releases/")
    maven("https://repo.momirealms.net/snapshots/")
}

dependencies {
    compileOnly("com.google.code.gson:gson:2.13.2")
    compileOnly("org.jetbrains:annotations:26.0.2")
    compileOnly("net.momirealms:craft-engine-core:26.3-SNAPSHOT")
    compileOnly("net.momirealms:craft-engine-bukkit:26.3-SNAPSHOT")
    compileOnly("it.unimi.dsi:fastutil:8.5.18")
    // Amazon S3
    compileOnly("software.amazon.awssdk:s3:2.38.7")
    compileOnly("software.amazon.awssdk:netty-nio-client:2.38.7")
    // Cache
    compileOnly("com.github.ben-manes.caffeine:caffeine:3.2.3")
    // bucket4j
    compileOnly("com.bucket4j:bucket4j_jdk17-core:8.15.0")
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks {
    shadowJar {
        archiveClassifier = ""
        archiveFileName = "craft-engine-s3-$version.jar"
    }
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release.set(21)
}

publishing {
    repositories {
        maven {
            url = uri("https://repo.momirealms.net/releases")
            credentials(PasswordCredentials::class) {
                username = System.getenv("REPO_USERNAME")
                password = System.getenv("REPO_PASSWORD")
            }
        }
    }
    publications {
        create<MavenPublication>("mavenJava") {
            groupId = "net.momirealms"
            artifactId = "craft-engine-s3"
            from(components["shadow"])
            pom {
                name = "CraftEngine S3"
                url = "https://github.com/Xiao-MoMi/craft-engine"
                licenses {
                    license {
                        name = "GNU General Public License v3.0"
                        url = "https://www.gnu.org/licenses/gpl-3.0.html"
                        distribution = "repo"
                    }
                }
            }
        }
    }
}