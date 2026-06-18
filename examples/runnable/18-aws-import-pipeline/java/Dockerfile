# Lab 6.4: a small, outbound-only Worker image. Multi-stage so the runtime layer
# ships a JRE + the fat jar, not Maven and the JDK.

# ---- build stage ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

# ---- runtime stage ----
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/worker.jar /app/worker.jar
# Container-aware heap: derive it from the container memory limit (K8s resources.limits),
# never a fixed -Xmx. No inbound port to EXPOSE — the Worker only dials out.
ENV JAVA_TOOL_OPTIONS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75"
ENTRYPOINT ["java", "-jar", "/app/worker.jar"]
