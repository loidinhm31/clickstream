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
