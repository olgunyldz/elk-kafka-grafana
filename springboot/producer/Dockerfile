# Build context is the parent folder (springboot/), because the module needs the parent pom:
#   docker build -f springboot/producer/Dockerfile springboot

# ---- 1. Build the jar ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
# Maven needs every module's pom listed in the parent, even when building only one module
COPY pom.xml .
COPY producer/pom.xml producer/
COPY consumer/pom.xml consumer/
COPY producer/src producer/src
# The cache mount keeps downloaded dependencies between builds
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -pl producer -am package -DskipTests \
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

ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75"
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "application.jar"]
