package com.ahmetkeles.e2e;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

/**
 * The shared five-container stack: PostgreSQL for metadata, three storage
 * nodes, and the metadata service — the services running as black-box
 * containers built from their boot jars. Started once per JVM and shared by
 * every test class; tests isolate by unique object keys, never by cleanup.
 *
 * <p>The chunk size is shrunk via environment (64 KiB) so modest payloads
 * exercise real multi-chunk, multi-node objects.
 */
final class E2eStack {

    static final int CHUNK_SIZE = 65536;

    private static final String POSTGRES_IMAGE = "postgres:17-alpine";
    private static final String JRE_IMAGE = "eclipse-temurin:21-jre";
    private static final String DB_PASSWORD = "e2e";

    private static E2eStack instance;

    final PostgreSQLContainer<?> metadataDb;
    final Map<String, GenericContainer<?>> storageNodes =
            new LinkedHashMap<>();
    final GenericContainer<?> metadataApp;

    static synchronized E2eStack get() {
        if (instance == null) {
            instance = new E2eStack();
        }
        return instance;
    }

    private final String subnetPrefix;

    private E2eStack() {
        // Dedicated subnet so the storage nodes can hold STATIC IPs. With
        // dynamic addressing, Docker hands a restarted container the lowest
        // free IP — and this suite's stop-two/start-two repair choreography
        // then SWAPS the two nodes' IPs about half the time, leaving stale
        // alias resolutions and neighbor caches routing one node's traffic
        // to the other for tens of seconds. A pinned IP per node removes
        // the swap entirely: a restarted node is always exactly where it
        // was.
        Network network = null;
        String prefix = null;
        RuntimeException lastFailure = null;
        for (int attempt = 0; attempt < 5 && network == null; attempt++) {
            // Random third octet: a fixed subnet collides with the previous
            // run's network when suites run back to back (Ryuk removes it
            // only seconds after the earlier JVM exits).
            String candidate = "10.213." + java.util.concurrent
                    .ThreadLocalRandom.current().nextInt(256) + ".";
            Network candidateNetwork = Network.builder()
                    .createNetworkCmdModifier(cmd -> cmd.withIpam(
                            new com.github.dockerjava.api.model
                                    .Network.Ipam()
                                    .withConfig(new com.github.dockerjava.api
                                            .model.Network.Ipam.Config()
                                            .withSubnet(candidate + "0/24"))))
                    .build();
            try {
                candidateNetwork.getId();
                network = candidateNetwork;
                prefix = candidate;
            } catch (RuntimeException exception) {
                lastFailure = exception;
            }
        }
        if (network == null) {
            throw new IllegalStateException(
                    "Could not create a network with a free /24 subnet",
                    lastFailure);
        }
        subnetPrefix = prefix;

        metadataDb = new PostgreSQLContainer<>(POSTGRES_IMAGE)
                .withDatabaseName("objectstore")
                .withUsername("objectstore_user")
                .withPassword(DB_PASSWORD)
                .withNetwork(network)
                .withNetworkAliases("metadata-db");

        for (int i = 1; i <= 3; i++) {
            String alias = "storage-node-" + i;
            // High static addresses (.101+) stay clear of the low range the
            // subnet's allocator hands to the db and the metadata app.
            String staticIp = subnetPrefix + (100 + i);
            storageNodes.put("node-" + i, appContainer(
                    network, alias, "storage-node", Map.of(
                            "STORAGE_NODE_PORT", "9000",
                            "STORAGE_NODE_DATA_DIR", "/data"
                    ))
                    .withCreateContainerCmdModifier(
                            cmd -> cmd.withIpv4Address(staticIp))
                    .withExposedPorts(9000)
                    .waitingFor(Wait.forHttp("/actuator/health")
                            .forPort(9000).forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(3))));
        }

        Startables.deepStart(
                Stream.concat(Stream.of(metadataDb),
                        storageNodes.values().stream()).toList()).join();

        Map<String, String> metadataEnv = new LinkedHashMap<>(Map.of(
                "METADATA_POSTGRES_HOST", "metadata-db",
                "METADATA_POSTGRES_PORT", "5432",
                "METADATA_POSTGRES_DB", "objectstore",
                "METADATA_POSTGRES_USER", "objectstore_user",
                "METADATA_POSTGRES_PASSWORD", DB_PASSWORD,
                "STORAGE_CHUNK_SIZE_BYTES", String.valueOf(CHUNK_SIZE),
                // Tight sweep interval so the repair e2e test observes
                // recovery in seconds; repair only ever acts on degraded
                // chunks, so the other test classes are unaffected by it.
                "STORAGE_REPAIR_INTERVAL", "PT2S"
        ));
        for (int i = 1; i <= 3; i++) {
            metadataEnv.put("STORAGE_NODES_" + (i - 1) + "_ID", "node-" + i);
            metadataEnv.put("STORAGE_NODES_" + (i - 1) + "_BASE_URL",
                    "http://storage-node-" + i + ":9000");
        }

        metadataApp = appContainer(
                network, "metadata-service", "metadata-service", metadataEnv)
                .withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/actuator/health")
                        .forPort(8080).forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));

        metadataApp.start();
    }

    String apiBaseUrl() {
        return "http://" + metadataApp.getHost() + ":"
                + metadataApp.getMappedPort(8080);
    }

    /**
     * Host-reachable base URL of one storage node, for direct inspection.
     * Resolved by live inspection rather than the cached mapping: a node
     * that has been stopped and started again gets a fresh ephemeral host
     * port from Docker, so the port captured at first startup goes stale.
     */
    String nodeBaseUrl(String nodeId) {
        GenericContainer<?> node = storageNodes.get(nodeId);

        Ports.Binding[] bindings = node.getDockerClient()
                .inspectContainerCmd(node.getContainerId()).exec()
                .getNetworkSettings().getPorts().getBindings()
                .get(ExposedPort.tcp(9000));

        if (bindings == null || bindings.length == 0) {
            throw new IllegalStateException(
                    "Node " + nodeId + " has no host binding for port 9000 "
                            + "(is the container running?)");
        }

        return "http://" + node.getHost() + ":"
                + bindings[0].getHostPortSpec();
    }

    /**
     * Stops the node's container process without removing the container, so
     * it can be started again with its data, network alias, and host port
     * binding intact.
     */
    void stopNode(String nodeId) {
        GenericContainer<?> node = storageNodes.get(nodeId);
        node.getDockerClient()
                .stopContainerCmd(node.getContainerId()).exec();
    }

    /**
     * Starts the node's container and blocks until it is addressable again
     * from inside the cluster network. Docker registers the container's
     * embedded-DNS record asynchronously — the alias can stay unresolvable
     * for several seconds after {@code docker start} returns — so "started"
     * here means the coordinator's own network namespace resolves the node
     * again, not merely that the container process exists.
     */
    void startNode(String nodeId) {
        GenericContainer<?> node = storageNodes.get(nodeId);
        node.getDockerClient()
                .startContainerCmd(node.getContainerId()).exec();

        String alias = "storage-" + nodeId;
        await().atMost(Duration.ofMinutes(2))
                .pollInterval(Duration.ofMillis(250))
                .ignoreExceptions()
                .until(() -> metadataApp.execInContainer(
                                "bash", "-lc", "getent hosts " + alias)
                        .getExitCode() == 0);
    }

    private static GenericContainer<?> appContainer(
            Network network,
            String alias,
            String service,
            Map<String, String> env
    ) {
        return new GenericContainer<>(DockerImageName.parse(JRE_IMAGE))
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withCopyFileToContainer(
                        MountableFile.forHostPath(resolveBootJar(service)),
                        "/app.jar")
                .withEnv(env)
                .withCommand("java", "-jar", "/app.jar")
                .withLogConsumer(new Slf4jLogConsumer(
                        LoggerFactory.getLogger("container." + alias)));
    }

    /**
     * The services are consumed as built jar files, not Maven dependencies.
     * Override with -De2e.&lt;service&gt;.jar=/path/to.jar; by default the
     * newest boot jar in the service's target directory is used.
     */
    private static Path resolveBootJar(String service) {
        String override = System.getProperty("e2e." + service + ".jar");
        if (override != null && !override.isBlank()) {
            return Path.of(override);
        }

        Path target = Path.of("..", "services", service, "target");
        try (Stream<Path> files = Files.list(target)) {
            return files
                    .filter(path -> {
                        String name = path.getFileName().toString();
                        return name.endsWith(".jar")
                                && !name.endsWith(".jar.original");
                    })
                    .max((a, b) -> {
                        try {
                            return Files.getLastModifiedTime(a)
                                    .compareTo(Files.getLastModifiedTime(b));
                        } catch (IOException exception) {
                            return 0;
                        }
                    })
                    .orElseThrow(() -> missingJar(service, target));
        } catch (IOException exception) {
            throw missingJar(service, target);
        }
    }

    private static IllegalStateException missingJar(String service,
                                                    Path target) {
        return new IllegalStateException(
                "No boot jar found in " + target.toAbsolutePath().normalize()
                        + ". Build it first: (cd services/" + service
                        + " && ./mvnw -DskipTests package)");
    }
}
