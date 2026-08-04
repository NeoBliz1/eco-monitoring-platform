package history.infrastructure.adapter.inbound;

import static history.infrastructure.adapter.inbound.HistoricalTelemetryListenerIT.ENVIRONMENT;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.INTERVAL_MINUTES;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.getProducerConf;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.getTestKafkaAdminConf;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.waitForConsulServicesToBeHealthy;
import static org.hibernate.jpa.SpecHints.HINT_SPEC_FETCH_GRAPH;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityGraph;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.DirectoryResourceAccessor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.history.HistoricalBootEngine;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.outbound.HistoricalQueryRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalQueryJpaRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
@ActiveProfiles({ "dev", "common" })
@SpringBootTest(classes = HistoricalBootEngine.class)
@TestPropertySource(locations = "classpath:.env.test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class IntegrationTestSupport {

    public static final String PG_DB = "pg-db";
    public static final int PG_DB_PORT = 5432;
    protected Producer<String, WeatherMap> testProducer;
    @Value("${spring.kafka.topic.weather-history}")
    String kafkaHistoryTopic;
    @Autowired
    HistoricalQueryRepository queryRepositoryAdapter;
    @Autowired
    HistoricalQueryJpaRepository queryJpaRepositoryAdapter;
    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;
    @PersistenceContext
    private EntityManager entityManager;
    @Autowired
    private KafkaProperties kafkaProperties;

    @BeforeAll
    static void beforeAll() {
        waitForConsulServicesToBeHealthy(List.of(
                "kafka",
                "schema-registry",
                "consul",
                "postgres"
        ));
    }

    static void runLiquibaseMigrationsOnTestComposeCluster() {
        log.info("⚙️ Extracting network configurations out of active Compose mapping...");
        String composePostgresHost = ENVIRONMENT.getServiceHost(PG_DB, PG_DB_PORT);
        Integer composePostgresPort = ENVIRONMENT.getServicePort(PG_DB, PG_DB_PORT);
        String dbName = "eco_platform_history_service_db";
        String schemaName = "history_service";
        String ecoUserPassword = "my_super_secret_eco_user_password";
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", composePostgresHost, composePostgresPort, dbName);
        File changelogDir = new File(System.getProperty("user.dir")+"/../docker/deployments/liquibase");
        try(Connection connection = DriverManager.getConnection(jdbcUrl, "eco_user", ecoUserPassword)) {
            Database database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(connection));
            database.setLiquibaseSchemaName(schemaName);
            database.setDefaultSchemaName(schemaName);
            try(DirectoryResourceAccessor resourceAccessor = new DirectoryResourceAccessor(changelogDir)) {
                String masterChangelogFilename = "db.changelog-master.yaml";
                Liquibase liquibaseEngine = new Liquibase(masterChangelogFilename, resourceAccessor, database);
                log.info("🚀 Updating target integration tables layout: {}.{}", dbName, schemaName);
                liquibaseEngine.update(new Contexts("test"), new LabelExpression());
                log.info("🌟 SUCCESS: All Liquibase database migrations applied cleanly to Compose stack!");
            }
        } catch(Exception e) {
            log.error("❌ Testcontainers Compose migration step collapsed! Integration pipeline aborted.", e);
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    public void setupEcosystem() {
        setupKafkaProducer();
    }

    @AfterEach
    public void teardownEcosystem() throws ExecutionException, InterruptedException {
        clearKafkaTopics();
        if(testProducer!=null) testProducer.close();
        queryJpaRepositoryAdapter.deleteAllInBatch();
    }

    private void setupKafkaProducer() {
        String bootstrapServersCsv = String.join(",", kafkaProperties.getBootstrapServers());
        Map<String, Object> producerProps = getProducerConf("client", "client-secret-pass",
                bootstrapServersCsv, schemaRegistryUrl);
        testProducer = new KafkaProducer<>(producerProps);
    }

    void clearKafkaTopics() throws InterruptedException, ExecutionException {
        List<String> bootstrapServersList = kafkaProperties.getBootstrapServers();
        String bootstrapServersCsv = String.join(",", bootstrapServersList);
        Map<String, Object> adminConf = getTestKafkaAdminConf("admin", "admin-password", bootstrapServersCsv);

        try(AdminClient adminClient = AdminClient.create(adminConf)) {
            List<String> topicsToClear = List.of(kafkaHistoryTopic);
            adminClient.deleteTopics(topicsToClear).all().get();
            Thread.sleep(300);
            NewTopic historyTopic = new NewTopic(kafkaHistoryTopic, 6, (short) 3)
                    .configs(Map.of(
                            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2",
                            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                            TopicConfig.RETENTION_MS_CONFIG, "-1"
                    ));
            adminClient.createTopics(List.of(historyTopic)).all().get();
            Thread.sleep(200);
        }
    }

    protected void sendPacket(long currentBucketFloor, WeatherMap weatherMap) throws Exception {
        testProducer.send(new ProducerRecord<>(
                kafkaHistoryTopic,
                0,
                currentBucketFloor,
                String.valueOf(currentBucketFloor),
                weatherMap
        )).get();
    }

    WeatherMapBucket getWeatherMapBucket(long timestampBucket) {
        try {
            EntityGraph<WeatherMapBucket> graph = entityManager.createEntityGraph(WeatherMapBucket.class);
            graph.addSubgraph("gridCells");
            Optional<WeatherMapBucket> shallowBucket = queryRepositoryAdapter
                    .findByTimestampBucketAndIntervalMinutes(timestampBucket, INTERVAL_MINUTES);
            assertTrue(shallowBucket.isPresent());
            UUID bucketId = shallowBucket.get().getId();
            return entityManager.find(
                    WeatherMapBucket.class,
                    bucketId,
                    Map.of(HINT_SPEC_FETCH_GRAPH, graph)
            );
        } finally {
            entityManager.clear();
        }
    }
}
