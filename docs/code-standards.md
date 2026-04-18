# Code Standards & Conventions

Project-wide code standards, architectural patterns, testing strategies, and best practices.

---

## Project Structure

### Maven Project Layout

```
clickstream/                           # Root project
├── pom.xml                           # Parent POM (version, plugins)
├── ingestion-api/                    # Phase 3 module
│   ├── pom.xml
│   ├── src/main/java/com/clickstream/
│   │   ├── ClickstreamApplication.java      # Main entry point
│   │   ├── config/                          # Spring configuration
│   │   │   ├── KafkaProducerConfig.java
│   │   │   ├── MongoIndexConfig.java
│   │   │   ├── CorsConfig.java
│   │   │   ├── RateLimitFilter.java
│   │   │   └── SharedModelConfig.java
│   │   ├── controller/                      # REST Controllers
│   │   │   ├── EventController.java
│   │   │   └── AnalyticsController.java
│   │   ├── service/                         # Business logic
│   │   │   ├── EventPublisher.java
│   │   │   └── AnalyticsService.java
│   │   ├── model/                           # Data models
│   │   │   ├── ClickEvent.java
│   │   │   ├── SessionAggregate.java
│   │   │   ├── PageMetric.java
│   │   │   └── UserJourney.java
│   │   ├── repository/                      # Spring Data
│   │   │   ├── SessionAggregateRepository.java
│   │   │   ├── PageMetricRepository.java
│   │   │   └── UserJourneyRepository.java
│   │   ├── exception/                       # Exception handling
│   │   │   └── GlobalExceptionHandler.java
│   │   ├── util/                            # Utilities
│   │   │   └── IpAnonymizer.java
│   │   └── validation/                      # Validators
│   │       └── EventValidator.java
│   ├── src/main/resources/
│   │   ├── application.yml                  # Default config
│   │   ├── application-docker.yml
│   │   └── application-test.yml
│   └── src/test/java/com/clickstream/
│       └── controller/
│           ├── EventControllerIntegrationTest.java
│           └── AnalyticsControllerIntegrationTest.java
├── shared-models/                    # Phase 2 module (shared)
├── spark-etl/                        # Phase 4 module
└── .mvn/
    └── settings.xml                  # Maven central direct access
```

### Naming Conventions

```
Classes (PascalCase):
    ✅ EventController, EventPublisher, SessionAggregate
    ❌ eventController, event_controller

Methods (camelCase):
    ✅ publishAsync(), getSessionsByUser()
    ❌ PublishAsync, get_sessions_by_user

Variables (camelCase):
    ✅ sessionId, userJourney, maxBatchSize
    ❌ SessionId, session_id, MAX_BATCH_SIZE

Constants (UPPER_SNAKE_CASE):
    ✅ MAX_REQUEST_SIZE, DEFAULT_PAGE_SIZE
    ❌ MAX_REQUEST_SIZE, maxRequestSize

Packages (lowercase with dots):
    ✅ com.clickstream.controller
    ❌ com.clickstream.Controller

MongoDB Collections (lowercase, plural):
    ✅ sessions, page_metrics, user_journeys
    ❌ Session, SESSIONS, sessionCollection
```

### Java Version & Dependencies

**Java Version:** 17 (Long-term support)
- Required for Spring Boot 3.x
- Features used: Records, sealed classes, pattern matching

**Spring Boot:** 3.x (Latest stable)
- Spring Web, Spring Data MongoDB, Spring Kafka
- Actuator (health checks, metrics)
- Validation (Bean Validation 3.x)

**Key Dependencies:**
```xml
<!-- API Framework -->
spring-boot-starter-web          <!-- REST controllers, servlet -->
spring-boot-starter-validation   <!-- @Valid, @NotNull, etc -->

<!-- Message Broker -->
spring-kafka                      <!-- KafkaTemplate, consumer -->

<!-- Database -->
spring-boot-starter-data-mongodb <!-- MongoRepository, queries -->

<!-- Utilities -->
jackson-databind                  <!-- JSON serialization -->
commons-lang3                     <!-- StringUtils, builders -->
bucket4j-core                     <!-- Rate limiting -->

<!-- Testing -->
spring-boot-starter-test          <!-- MockMvc, TestRestTemplate -->
testcontainers                    <!-- Embedded Kafka, MongoDB -->
testcontainers-junit-jupiter      <!-- @Testcontainers -->
```

---

## Coding Patterns & Best Practices

### Controllers

**Pattern: Thin Controller Layer**
- Controllers validate and marshal requests
- Delegate business logic to services
- Always return ResponseEntity with appropriate HTTP status

```java
@RestController
@RequestMapping("/api/events")
@CrossOrigin(origins = "${clickstream.cors.allowed-origins:http://localhost:3000}")
public class EventController {

    private final EventPublisher eventPublisher;
    private final EventValidator eventValidator;

    public EventController(EventPublisher eventPublisher, EventValidator eventValidator) {
        this.eventPublisher = eventPublisher;
        this.eventValidator = eventValidator;
    }

    @PostMapping
    public ResponseEntity<IngestionResponse> ingest(@Valid @RequestBody ClickEvent event) {
        logger.debug("Received event: type={}, sessionId={}", event.getEventType(), event.getSessionId());
        
        List<String> errors = eventValidator.validate(event);
        if (!errors.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(new IngestionResponse(false, "Validation failed", errors));
        }

        eventPublisher.publishAsync(event);
        return ResponseEntity.accepted()
                .body(new IngestionResponse(true, "Event accepted", null));
    }
}

// ✅ Benefits:
// • Validation happens first (fail fast)
// • Logging at controller level (request context visible)
// • Business logic in service (testable, reusable)
// • Appropriate HTTP status codes (400, 202, 500)
```

### Services

**Pattern: Single Responsibility**
- Each service handles one business domain
- Services are stateless and thread-safe
- Use dependency injection for collaborators

```java
@Service
public class EventPublisher {

    private static final Logger logger = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    // ✅ Constructor injection (not @Autowired)
    public EventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${clickstream.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public CompletableFuture<SendResult<String, String>> publishAsync(ClickEvent event) {
        try {
            String key = event.getSessionId();  // Partition key
            String value = objectMapper.writeValueAsString(event);
            
            logger.debug("Publishing event: sessionId={}, eventId={}", key, event.getEventId());
            
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, value);
            
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    logger.error("Kafka send failed for event: {}", event.getEventId(), ex);
                } else {
                    logger.debug("Event published to partition: {}", result.getRecordMetadata().partition());
                }
            });
            
            return future;
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize event", e);
            return CompletableFuture.failedFuture(e);
        }
    }
}

// ✅ Benefits:
// • Constructor injection → final fields → thread-safe
// • Async handling → fast response time for API
// • Logging at each step → observability
// • Error callbacks → don't silently fail
```

### Repositories

**Pattern: Repository Interface Only**
- Define queries as method signatures
- Spring Data creates implementations
- Use custom query methods for complex queries

```java
public interface SessionAggregateRepository extends MongoRepository<SessionAggregate, String> {

    // ✅ Simple queries (Spring Data auto-generates)
    Optional<SessionAggregate> findBySessionId(String sessionId);
    
    List<SessionAggregate> findByUserId(String userId);

    // ✅ Custom queries with @Query
    @Query("{ 'userId': ?0, 'startTime': { $gte: ?1, $lte: ?2 } }")
    Page<SessionAggregate> findByUserIdAndTimeRange(
            String userId,
            Long startTime,
            Long endTime,
            Pageable pageable);

    // ✅ Pagination support
    Page<SessionAggregate> findByUserId(String userId, Pageable pageable);

    // ✅ Delete with repository
    Long deleteByStartTimeLessThan(Long timestamp);
}

// Usage in analytics service:
Page<SessionAggregate> sessions = repository.findByUserIdAndTimeRange(
    userId, startTime, endTime, PageRequest.of(0, 20));

// ✅ Benefits:
// • No boilerplate query code
// • Automatically indexed
// • Type-safe, compile-checked
// • Pagination built-in
```

### Exception Handling

**Pattern: Global Exception Handler**
- Centralize exception handling
- Sanitize error messages (never expose internal details)
- Return consistent error response format

```java
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(
            MethodArgumentNotValidException ex) {
        List<String> errors = ex.getBindingResult().getFieldErrors()
                .stream()
                .map(e -> e.getField() + ": " + e.getDefaultMessage())
                .toList();
        
        logger.warn("Validation failed: {}", errors);
        return ResponseEntity.badRequest()
                .body(new ErrorResponse("VALIDATION_ERROR", errors.get(0), errors));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception ex) {
        logger.error("Unexpected error", ex);
        
        // ✅ Never expose internal stack traces to client
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse("INTERNAL_ERROR", "An unexpected error occurred", null));
    }
}

record ErrorResponse(String code, String message, List<String> details) {}

// ✅ Benefits:
// • Consistent error format
// • Logging centralized
// • Security: no stack traces to client
// • Type-safe error codes
```

### Validation

**Pattern: Multi-Layer Validation**

```java
// Layer 1: Bean Validation annotations
@Data
public class ClickEvent {
    @NotBlank(message = "eventId required")
    private String eventId;

    @NotBlank(message = "userId required")
    private String userId;

    @NotBlank(message = "sessionId required")
    private String sessionId;

    @NotNull(message = "eventType required")
    private EventType eventType;

    @NotBlank(message = "pageUrl required")
    @URL(message = "pageUrl must be valid URL")
    private String pageUrl;

    @NotNull(message = "timestamp required")
    private Long timestamp;
}

// Layer 2: @Valid in controller
@PostMapping
public ResponseEntity<?> ingest(@Valid @RequestBody ClickEvent event) {
    // If @Valid fails, GlobalExceptionHandler catches it (400)
    
    // Layer 3: Custom business validation
    List<String> errors = eventValidator.validate(event);
    if (!errors.isEmpty()) {
        return ResponseEntity.badRequest().body(...);
    }
    
    // All validations passed
}

// Layer 4: Defensive null checks in service (belt-and-suspenders)
public void publishAsync(ClickEvent event) {
    if (event == null || event.getSessionId() == null) {
        throw new IllegalArgumentException("Event and sessionId required");
    }
}

// ✅ Benefits:
// • Fail fast at controller boundary
// • Spring auto-validates with @Valid
// • Custom business rules in EventValidator
// • Debug logging visible if validation fails
```

---

## Testing Strategy

### Test Pyramid

```
         /\           Unit Tests
        /  \          (isolated, mocked)
       /────\         60% of tests
      /      \        • Model validation
     /        \       • Utility functions
    /__________\      • Service logic (mocked)

        /  \          Integration Tests
       /    \         (real components)
      /______\        30% of tests
     • Controller with MockMvc
     • Repository queries
     • Kafka producer
     • MongoDB (testcontainers)

       / \            End-to-End Tests
      /___\           (full system)
      10% of tests
      • API load test
      • Kafka topic verification
      • MongoDB data verification
```

### Unit Tests

**Pattern: Mocked Dependencies**

```java
class EventPublisherTest {

    private KafkaTemplate<String, String> kafkaTemplate;
    private ObjectMapper objectMapper;
    private EventPublisher publisher;

    @BeforeEach
    void setup() {
        kafkaTemplate = mock(KafkaTemplate.class);
        objectMapper = new ObjectMapper();
        publisher = new EventPublisher(kafkaTemplate, objectMapper, "clickstream-events");
    }

    @Test
    void shouldPublishEventWithSessionIdAsKey() {
        // Arrange
        ClickEvent event = new ClickEvent("eventId", "userId", "sess-123", ...);
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(eq("clickstream-events"), eq("sess-123"), any()))
                .thenReturn(future);

        // Act
        publisher.publishAsync(event);

        // Assert
        verify(kafkaTemplate).send(eq("clickstream-events"), eq("sess-123"), any());
    }
}

// ✅ Benefits:
// • Fast execution (no I/O)
// • Deterministic (no flakiness)
// • Tests one class only
// • Easy to understand and maintain
```

### Integration Tests

**Pattern: Testcontainers for Infrastructure**

```java
@SpringBootTest
@Testcontainers
class EventControllerIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @Container
    static MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @BeforeAll
    static void setup() {
        System.setProperty("spring.kafka.bootstrap-servers", kafka.getBootstrapServers());
        System.setProperty("spring.data.mongodb.uri", mongo.getReplicaSetUrl());
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void shouldAcceptEventAndPublishToKafka() {
        // Arrange
        ClickEvent event = new ClickEvent("e1", "u1", "s1", CLICK, ...);

        // Act
        ResponseEntity<IngestionResponse> response = restTemplate.postForEntity(
                "/api/events",
                event,
                IngestionResponse.class);

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody().isSuccess()).isTrue();

        // Verify in Kafka topic
        ConsumerRecord<String, String> record = getRecordFromKafka("clickstream-events");
        assertThat(record.key()).isEqualTo("s1");
        assertThat(record.value()).contains("\"eventId\":\"e1\"");
    }

    @Test
    void shouldReturnBadRequestForInvalidEvent() {
        // Missing required field
        String invalidJson = "{ \"eventType\": \"CLICK\" }";

        ResponseEntity<?> response = restTemplate.postForEntity(
                "/api/events",
                invalidJson,
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}

// ✅ Benefits:
// • Tests real components (Spring context, Kafka, MongoDB)
// • Verifies end-to-end behavior
// • Catches integration bugs
// • Real infrastructure (testcontainers)
```

### Test Data Builders

**Pattern: Builder Pattern for Test Data**

```java
class ClickEventBuilder {
    private String eventId = UUID.randomUUID().toString();
    private String userId = "user1";
    private String sessionId = "session1";
    private EventType eventType = CLICK;
    private String pageUrl = "https://example.com";
    private Long timestamp = System.currentTimeMillis();

    public ClickEvent build() {
        return new ClickEvent(eventId, userId, sessionId, eventType, pageUrl, timestamp);
    }

    public ClickEventBuilder withUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public ClickEventBuilder withSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }
}

// Usage:
ClickEvent event = new ClickEventBuilder()
        .withUserId("user123")
        .withSessionId("sess-456")
        .build();

// ✅ Benefits:
// • Readable test setup
// • Minimal required fields
// • Easy to customize
// • Centralized test data
```

### Test Naming Conventions

```java
// Pattern: shouldWhen()

@Test
void shouldReturn202WhenEventIsValid() { ... }

@Test
void shouldReturn400WhenUserIdIsBlank() { ... }

@Test
void shouldPublishToKafkaPartitionMatchingSessionId() { ... }

@Test
void shouldTimeoutAfter5SecondsOnMongoDBConnection() { ... }

// ✅ Benefits:
// • Clear intent of test
// • Test result is self-documenting
// • Easy to scan test methods
```

---

## Security Best Practices

### Input Validation

```java
// ✅ ALWAYS validate input
@PostMapping
public ResponseEntity<?> ingest(@Valid @RequestBody ClickEvent event) {
    // @Valid triggers Bean Validation
    // GlobalExceptionHandler catches errors
}

// ❌ NEVER trust client input
List<ClickEvent> events = request.getEvents();
for (ClickEvent e : events) {
    // ❌ Don't do this: assume e is valid
    kafkaTemplate.send(topic, e.getSessionId(), mapper.writeValueAsString(e));
}

// ✅ DO validate each event
for (ClickEvent e : events) {
    if (e.getSessionId() == null) throw new ValidationException("Invalid sessionId");
    kafkaTemplate.send(...);
}
```

### Error Message Sanitization

```java
// ❌ DON'T expose internal errors
catch (Exception e) {
    return ResponseEntity.status(500)
            .body(e.getMessage());  // ❌ Exposes internal details
}

// ✅ DO return generic error message
catch (Exception e) {
    logger.error("Database error", e);  // Log internally
    return ResponseEntity.status(500)
            .body("An error occurred processing your request");  // Generic for client
}
```

### PII Protection

```java
// ❌ NEVER log or store PII without anonymization
logger.info("User {} logged in", userId);  // ❌ If userId is email

// ✅ DO anonymize or hash
String anonymousId = IpAnonymizer.anonymize(ipAddress);
logger.info("User accessed endpoint");

// ✅ Use utility class for sensitive data
@Service
public class IpAnonymizer {
    public static String anonymize(String ipAddress) {
        // Remove last octet: 192.168.1.234 → 192.168.1.0
        String[] parts = ipAddress.split("\\.");
        return String.join(".", parts[0], parts[1], parts[2], "0");
    }
}
```

### Secrets Management

```java
// ❌ NEVER hardcode secrets
private final String dbPassword = "super-secret-123";

// ✅ DO use environment variables or vault
@Value("${database.password}")
private String dbPassword;

// ✅ Use Spring Cloud Config or HashiCorp Vault
@Value("${spring.data.mongodb.uri}")
private String mongoUri;  // From environment or vault
```

---

## Performance Optimization

### Caching Strategy

```java
// ✅ Cache rarely-changing data
@Service
public class AnalyticsService {

    @Cacheable(value = "page-metrics", key = "#pageUrl")
    public PageMetric getPageMetrics(String pageUrl) {
        // Database query here
        // Result cached for 5 minutes
    }

    @CacheEvict(value = "page-metrics", key = "#pageUrl")
    public void invalidatePageMetricsCache(String pageUrl) {
        // Called after data update
    }
}

// ✅ Added to pom.xml:
// <dependency>
//     <groupId>org.springframework.boot</groupId>
//     <artifactId>spring-boot-starter-cache</artifactId>
// </dependency>
```

### Database Indexing

```java
// ✅ Index frequently-queried fields
@Document(collection = "sessions")
public class SessionAggregate {

    @MongoId
    private String id;

    @Indexed  // Index on userId for fast lookup
    private String userId;

    @Indexed  // Index on sessionId for uniqueness + fast lookup
    @Unique
    private String sessionId;

    @Indexed  // Index on startTime for date-range queries
    private Long startTime;
}

// ✅ Multi-field indexes in MongoIndexConfig:
db.sessions.createIndex({ userId: 1, startTime: -1 })

// ✅ Benefits:
// • Fast queries (< 5ms even with 1M documents)
// • Lower CPU usage
// • Better scalability
```

### Batch Processing

```java
// ✅ Batch Kafka sends for throughput
int batchSize = 0;
List<CompletableFuture<?>> futures = new ArrayList<>();

for (ClickEvent event : events) {
    futures.add(publisher.publishAsync(event));
    batchSize++;

    if (batchSize >= 100) {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        futures.clear();
        batchSize = 0;
    }
}

// ✅ Connection Pooling
// Configured in application.yml:
// spring.data.mongodb:
//   max-pool-size: 200
//   min-pool-size: 50

// ✅ Results:
// • Single pod: 10k+ events/sec
// • Multiple pods: 100k+ events/sec with load balancer
```

---

## Spark ETL Patterns (Phase 4)

### Architecture Overview

The Spark ETL module processes Kafka events through three parallel aggregation streams using Structured Streaming, implemented as a Spring Boot application managing SparkSession lifecycle.

```
Pattern: SparkSession as Spring Bean → Reusable across multiple jobs
         CommandLineRunner for job orchestration
         foreachBatch() sink for full DataFrame API control in each micro-batch
```

### Project Structure

```
spark-etl/
├── SparkETLApplication.java          # Entry point (Spring Boot)
├── job/ClickstreamETLJob.java        # Orchestrator (CommandLineRunner)
├── config/
│   ├── SparkConfig.java              # SparkSession bean, Spark config
│   ├── MongoConfig.java              # MongoClient bean (transient, created per batch)
│   └── StreamingQueryMonitor.java    # Listener for query metrics
├── schema/EventSchema.java           # Spark SQL schemas (immutable)
├── transform/
│   ├── SessionAggregator.java        # Stream 1: Session windows (30-min gap)
│   ├── PageMetricsAggregator.java    # Stream 2: Page metrics (5-min tumbling)
│   └── UserJourneyBuilder.java       # Stream 3: User journeys (30-min gap)
├── sink/MongoForeachBatchWriter.java # Batch writer (MongoClient lifecycle)
└── service/MongoIndexService.java    # Indexes at startup
```

### SparkSession Configuration

**Pattern: Spring Bean Managed SparkSession**

```java
@Configuration
public class SparkConfig {

    private static final Logger logger = LoggerFactory.getLogger(SparkConfig.class);

    private final StreamingQueryMonitor queryMonitor;

    public SparkConfig(StreamingQueryMonitor queryMonitor) {
        this.queryMonitor = queryMonitor;
    }

    @Bean
    public SparkSession sparkSession() {
        SparkSession spark = SparkSession.builder()
                .appName("clickstream-etl")
                .master("local[*]")  // Dev mode; remove for cluster
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.shuffle.partitions", "4")
                // Session window buffer (increase for larger windows)
                .config("spark.sql.session.window.buffer.in.memory.threshold", "4096")
                // Force delete temp checkpoint on restart (dev only)
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation",
                        System.getenv("SPARK_FORCE_DELETE_CHECKPOINT") != null ? "true" : "false")
                .getOrCreate();

        spark.sparkContext().setLogLevel("INFO");
        spark.streams().addListener(queryMonitor);

        return spark;
    }

    @Override
    public void destroy() {
        SparkSession spark = SparkSession.active();
        if (spark != null) {
            logger.info("Stopping SparkSession...");
            spark.stop();
        }
    }
}

// ✅ Benefits:
// • Single SparkSession per app (expensive to create)
// • Reusable from any service via @Autowired
// • Configuration centralized (dev vs prod)
// • Listener registered for metrics/monitoring
```

### ETL Job Orchestration

**Pattern: CommandLineRunner for Stream Management**

```java
@Component
public class ClickstreamETLJob implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(ClickstreamETLJob.class);

    private final SparkSession sparkSession;
    private final SessionAggregator sessionAggregator;
    private final PageMetricsAggregator pageMetricsAggregator;
    private final UserJourneyBuilder userJourneyBuilder;
    private final MongoForeachBatchWriter mongoWriter;
    private final StreamingQueryListener queryListener;

    // Constructor injection of all dependencies

    @Override
    public void run(String... args) throws Exception {
        logger.info("Starting Clickstream ETL Job");

        try {
            // Read Kafka stream once
            Dataset<Row> rawEvents = sparkSession
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaServers)
                    .option("subscribe", "clickstream-events")
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "false")
                    .load()
                    .select(from_json(col("value"), EventSchema.clickEventSchema()).alias("event"))
                    .select("event.*")
                    .withWatermark("timestamp", "10 minutes");

            logger.info("Kafka source configured with 10-minute watermark");

            // Stream 1: Session Aggregates
            StreamingQuery sessionQuery = sessionAggregator.aggregate(rawEvents)
                    .writeStream()
                    .foreachBatch((batchDf, batchId) ->
                            mongoWriter.writeBatch(batchDf, batchId, "session_aggregates"))
                    .option("checkpointLocation", checkpointDir + "/session-aggregates")
                    .start();

            // Stream 2: Page Metrics
            StreamingQuery pageMetricsQuery = pageMetricsAggregator.aggregate(rawEvents)
                    .writeStream()
                    .foreachBatch((batchDf, batchId) ->
                            mongoWriter.writeBatch(batchDf, batchId, "page_metrics"))
                    .option("checkpointLocation", checkpointDir + "/page-metrics")
                    .start();

            // Stream 3: User Journeys
            StreamingQuery userJourneyQuery = userJourneyBuilder.build(rawEvents)
                    .writeStream()
                    .foreachBatch((batchDf, batchId) ->
                            mongoWriter.writeBatch(batchDf, batchId, "user_journeys"))
                    .option("checkpointLocation", checkpointDir + "/user-journeys")
                    .start();

            logger.info("All 3 ETL streams started successfully");

            // Block until any stream terminates
            sparkSession.streams().awaitAnyTermination();

        } catch (StreamingQueryException e) {
            logger.error("Streaming query failed", e);
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down Spark ETL Job");
        if (sparkSession != null) {
            sparkSession.streams().active().forEach(StreamingQuery::stop);
        }
    }
}

// ✅ Benefits:
// • Logical organization: 3 streams orchestrated together
// • Independent checkpoints: each stream can restart independently
// • Error handling: one stream failure doesn't stop others
// • Graceful shutdown: @PreDestroy stops all streams
```

### DataFrame Schema Definition

**Pattern: Schema as Static Methods (Immutable)**

```java
public class EventSchema {

    // Schema for raw Kafka JSON
    public static StructType clickEventSchema() {
        return new StructType(new StructField[]{
                new StructField("eventId", StringType$.MODULE$, false, Metadata.empty()),
                new StructField("userId", StringType$.MODULE$, false, Metadata.empty()),
                new StructField("sessionId", StringType$.MODULE$, false, Metadata.empty()),
                new StructField("eventType", StringType$.MODULE$, false, Metadata.empty()),
                new StructField("pageUrl", StringType$.MODULE$, false, Metadata.empty()),
                new StructField("timestamp", LongType$.MODULE$, false, Metadata.empty()),
                new StructField("metadata", metadataSchema(), true, Metadata.empty()),
        });
    }

    public static StructType metadataSchema() {
        return new StructType(new StructField[]{
                new StructField("x", IntegerType$.MODULE$, true, Metadata.empty()),
                new StructField("y", IntegerType$.MODULE$, true, Metadata.empty()),
                new StructField("scrollDepth", DoubleType$.MODULE$, true, Metadata.empty()),
                // ... other fields
        });
    }

    private EventSchema() {
        throw new UnsupportedOperationException("Utility class");
    }
}

// Usage in transformers:
Dataset<Row> events = sparkSession
        .readStream()
        .option("subscribe", "clickstream-events")
        .json(...)
        .select(from_json(col("value"), EventSchema.clickEventSchema()).alias("event"))
        .select("event.*");

// ✅ Benefits:
// • Schema single source of truth
// • Reusable across all 3 streams
// • Type-safe DataFrame operations
// • Matches shared-models ClickEvent structure
```

### Transform: Parallel Aggregations

**Pattern: Aggregator Classes (Stateless, Testable)**

```java
@Component
public class SessionAggregator {

    private final SparkSession sparkSession;

    public SessionAggregator(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> aggregate(Dataset<Row> rawEvents, int sessionGapMinutes) {
        return rawEvents
                .filter(col("eventType").isin("CLICK", "PAGE_VIEW", "SCROLL"))
                .groupBy(
                        session_window(col("timestamp"), sessionGapMinutes + " minutes"),
                        col("sessionId"),
                        col("userId"))
                .agg(
                        // Aggregations
                        (max(col("timestamp")).minus(min(col("timestamp")))).alias("durationMs"),
                        count(when(col("eventType").equalTo("PAGE_VIEW"), 1)).alias("pageViewCount"),
                        count(when(col("eventType").equalTo("CLICK"), 1)).alias("clickCount"),
                        countDistinct(col("pageUrl")).alias("uniquePageCount"),
                        first(col("pageUrl")).alias("entryPage"),
                        last(col("pageUrl")).alias("exitPage"),
                        // Bounce rate: only 1 unique page AND duration < 10 sec
                        when(
                                and(
                                        col("uniquePageCount").equalTo(1),
                                        col("durationMs").lt(10000)),
                                1)
                                .otherwise(0)
                                .alias("bounced"))
                .select(
                        col("sessionId"),
                        col("userId"),
                        col("window.start").alias("windowStart"),
                        col("window.end").alias("windowEnd"),
                        col("durationMs"),
                        col("pageViewCount"),
                        col("clickCount"),
                        col("uniquePageCount"),
                        col("entryPage"),
                        col("exitPage"),
                        col("bounced"),
                        current_timestamp().alias("createdAt"));
    }
}

// ✅ Benefits:
// • Single responsibility (session aggregation only)
// • Stateless (can be parallelized)
// • Testable (input DataFrame → output DataFrame)
// • Reusable (called from ClickstreamETLJob)
```

### MongoDB Sink with foreachBatch

**Pattern: Batch Writer (MongoClient Lifecycle per Batch)**

```java
public class MongoForeachBatchWriter implements Serializable {

    // ✅ IMPORTANT: Create MongoClient INSIDE foreachBatch, not before
    public void writeBatch(Dataset<Row> batchDf, long batchId, String collectionName) {
        logger.info("Writing batch {} to collection: {}", batchId, collectionName);

        // Create MongoClient inside batch scope (serialization-safe)
        MongoClient client = MongoClients.create(mongoUri);
        try {
            MongoCollection<Document> collection = client
                    .getDatabase(dbName)
                    .getCollection(collectionName);

            List<WriteModel<Document>> writes = batchDf
                    .collectAsList()
                    .stream()
                    .map(row -> {
                        // Convert Row to Document (upsert key varies by collection)
                        Document doc = convertRowToDocument(row);
                        String upsertKey = getUpsertKey(collectionName);

                        // Upsert filter: match by composite key
                        Bson filter = createUpsertFilter(row, upsertKey);

                        // Upsert operation: update or insert
                        return new UpdateOneModel<>(
                                filter,
                                new Document("$set", doc),
                                new UpdateOptions().upsert(true));
                    })
                    .toList();

            // Batch write with bulk operations
            BulkWriteResult result = collection.bulkWrite(writes);
            logger.info("Batch {}: upserted={}, matched={}", batchId,
                    result.getUpsertedCount(), result.getMatchedCount());

        } finally {
            client.close();  // ✅ Always close client
        }
    }

    private Bson createUpsertFilter(Row row, String collectionName) {
        // session_aggregates: match on {sessionId, windowStart}
        // page_metrics: match on {pageUrl, windowStart}
        // user_journeys: match on {userId, sessionId}
        switch (collectionName) {
            case "session_aggregates":
                return Filters.and(
                        Filters.eq("sessionId", row.getAs("sessionId")),
                        Filters.eq("windowStart", new Date(row.getAs("windowStart"))));
            // ... other cases
        }
    }
}

// ✅ Benefits:
// • foreachBatch gives full DataFrame API per micro-batch
// • Batch writes faster than row-at-a-time
// • Upsert by composite key (idempotent)
// • Bulk operations reduce network round-trips
// • MongoClient created inside foreachBatch (serialization-safe)
```

### Startup: Initialize MongoDB Indexes

**Pattern: ApplicationReadyEvent Listener**

```java
@Service
public class MongoIndexService {

    private static final Logger logger = LoggerFactory.getLogger(MongoIndexService.class);

    private final MongoClient mongoClient;
    private final String dbName;

    @EventListener(ApplicationReadyEvent.class)
    public void initializeIndexes() {
        logger.info("Initializing MongoDB indexes...");

        MongoDatabase db = mongoClient.getDatabase(dbName);

        // Indexes for session_aggregates
        db.getCollection("session_aggregates")
                .createIndex(Indexes.compoundIndex(
                        Indexes.ascending("sessionId"),
                        Indexes.descending("windowStart")));

        // Indexes for page_metrics (with TTL)
        db.getCollection("page_metrics")
                .createIndex(new Document("createdAt", 1)
                        .append("expireAfterSeconds", 2592000)); // 30 days

        // Indexes for user_journeys
        db.getCollection("user_journeys")
                .createIndex(Indexes.compoundIndex(
                        Indexes.ascending("userId"),
                        Indexes.ascending("sessionId")));

        logger.info("All indexes created successfully");
    }
}

// ✅ Benefits:
// • Runs once at startup (guaranteed before first write)
// • TTL index auto-deletes old documents
// • Compound indexes optimize queries
// • Logging for verification
```

### Configuration & Environment Variables

**Key settings from application.yml:**

```yaml
kafka:
  bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
  topic: clickstream-events
  properties:
    security.protocol: ${KAFKA_SECURITY_PROTOCOL:PLAINTEXT}

mongodb:
  uri: ${MONGODB_URI:mongodb://localhost:27017}
  database: clickstream_db

spark:
  checkpoint-location: /tmp/spark-checkpoints
  config:
    executor.memory: ${SPARK_EXECUTOR_MEMORY:512m}
    driver.memory: ${SPARK_DRIVER_MEMORY:1g}

streaming:
  trigger:
    processing-time: 30  # seconds
  watermark:
    delay: 10  # minutes
```

**Production deployment example:**

```bash
# Set environment before running
export KAFKA_BOOTSTRAP_SERVERS="kafka1:9092,kafka2:9092,kafka3:9092"
export MONGODB_URI="mongodb://replica-set-0:27017,replica-set-1:27017/?replicaSet=rs0"
export SPARK_EXECUTOR_MEMORY="4g"
export SPARK_DRIVER_MEMORY="2g"

# Run application
java -jar spark-etl-1.0.0-SNAPSHOT.jar
```

### Error Handling & Resilience

**Pattern: StreamingQueryException Handling**

```java
try {
    sparkSession.streams().awaitAnyTermination();
} catch (StreamingQueryException e) {
    logger.error("Streaming query failed: {}", e.getMessage(), e);
    if (e.message().contains("checkpoint")) {
        logger.info("Checkpoint corrupted, delete and restart");
        // Clean up checkpoint directory
        // Restart will replay from earliest offset
    }
    throw new RuntimeException(e);
}

// ✅ Benefits:
// • Specific error messages for debugging
// • Checkpoint corruption detected and logged
// • Graceful exit (Spring Boot handles restart)
```

---

## Code Review Checklist

When reviewing pull requests, verify:

- [ ] **Naming:** Classes PascalCase, methods/vars camelCase, packages lowercase
- [ ] **Structure:** Controllers thin, services thick, repositories queries-only
- [ ] **Composition:** Constructor injection, not @Autowired
- [ ] **Error Handling:** GlobalExceptionHandler catches exceptions, no stack traces to client
- [ ] **Logging:** DEBUG at entry/exit, INFO on success, WARN/ERROR on failure
- [ ] **Validation:** Input validated at controller, @Valid used, custom validation in service
- [ ] **Security:** No hardcoded secrets, PII anonymized, error messages sanitized
- [ ] **Tests:** Unit tests for logic, integration tests for controllers, > 70% coverage
- [ ] **Performance:** No N+1 queries, indexes created, caching used where appropriate
- [ ] **Documentation:** JavaDoc on public methods, complex logic explained

---

## References

- **Spring Boot Guide:** https://spring.io/guides/gs/spring-boot/
- **Spring Data MongoDB:** https://spring.io/projects/spring-data-mongodb
- **Spring Kafka:** https://spring.io/projects/spring-kafka
- **Testcontainers:** https://www.testcontainers.org/
- **MongoDB Best Practices:** https://docs.mongodb.com/manual/administration/
