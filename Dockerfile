# Production-ready Dockerfile for IoT Telemetry Pipeline
# Builds code and runs Kafka + Java application in a single resource-bounded container.

FROM eclipse-temurin:17-jdk-alpine

LABEL maintainer="IoT Telemetry Pipeline"
LABEL description="IoT Telemetry Pipeline - Containerized Single Node Service"

# Install dependencies (bash, curl, tar, findutils for Kafka, and maven to build)
RUN apk add --no-cache bash curl tar findutils maven

# Download and install Apache Kafka 3.7.0
ENV KAFKA_VERSION=3.7.0
ENV SCALA_VERSION=2.13
RUN wget https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt && \
    mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka && \
    rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz

WORKDIR /app

# Copy pom.xml and source code
COPY pom.xml /app/
COPY src /app/src
COPY scripts /app/scripts
COPY README.md /app/README.md

# Build the application fat JAR
RUN mvn clean package -DskipTests

# Copy the compiled JAR to the standard location
RUN cp target/iot-telemetry-pipeline-1.0-SNAPSHOT-jar-with-dependencies.jar /app/app.jar

# Create a non-root group and user for execution security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup && \
    chown -R appuser:appgroup /app && \
    chown -R appuser:appgroup /opt/kafka

# Fix line endings and make the entrypoint script executable
RUN sed -i 's/\r$//' /app/scripts/entrypoint.sh && \
    chmod +x /app/scripts/entrypoint.sh

# Switch to the non-root user
USER appuser

# Expose ports (Kafka default listener)
EXPOSE 9092

# Run entrypoint
ENTRYPOINT ["/app/scripts/entrypoint.sh"]

# Default command if no arguments are passed
CMD ["help"]
