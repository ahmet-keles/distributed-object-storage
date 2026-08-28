# Runtime image for a storage node. Build the boot jar first:
#   (cd services/storage-node && ./mvnw -DskipTests package)
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/storage-node-*.jar app.jar
VOLUME /data
ENV STORAGE_NODE_DATA_DIR=/data
EXPOSE 9000
ENTRYPOINT ["java", "-jar", "app.jar"]
