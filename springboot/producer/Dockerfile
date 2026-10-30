# Build context is the parent folder (springboot/), because the module needs the parent pom:
#   docker build -f springboot/producer/Dockerfile springboot

# ---- 1. Build the jar ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
# Maven needs every module's pom listed in the parent, even when building only one module
COPY pom.xml .
COPY log-masking/pom.xml log-masking/
COPY producer/pom.xml producer/
COPY consumer/pom.xml consumer/
COPY message-api/pom.xml message-api/

# 1a. Build and install the log-masking library first, with its tests: the app depends on it.
#     The cache mount keeps downloaded dependencies and the installed library between builds.
COPY log-masking/src log-masking/src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -pl log-masking -am install

# 1b. Build the app against the installed library
COPY producer/src producer/src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -pl producer package -DskipTests \
 && cp producer/target/producer-*.jar /application.jar

# ---- 2. Split the jar into layers ----
# Dependencies rarely change, so Docker can reuse that layer when only our code changes
FROM eclipse-temurin:21-jre AS extract
WORKDIR /extract
COPY --from=build /application.jar application.jar
RUN java -Djarmode=tools -jar application.jar extract --layers --destination extracted

# ---- 3. Runtime image ----
FROM eclipse-temurin:21-jre
WORKDIR /app
RUN groupadd --system spring && useradd --system --gid spring spring
COPY --from=extract /extract/extracted/dependencies/ ./
COPY --from=extract /extract/extracted/spring-boot-loader/ ./
COPY --from=extract /extract/extracted/snapshot-dependencies/ ./
COPY --from=extract /extract/extracted/application/ ./
USER spring

EXPOSE 8081
# JVM flags on the command line, not in JAVA_TOOL_OPTIONS: the JVM would announce that on stderr as a
# plain-text line, and every log line must be JSON for Filebeat
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "application.jar"]
