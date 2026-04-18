package com.clickstream.config;

import com.clickstream.model.PageMetric;
import com.clickstream.model.SessionAggregate;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;

/**
 * MongoDB index configuration.
 * 
 * Creates indexes manually instead of relying on auto-index-creation.
 * Auto-index only works in dev mode and can cause performance issues in production.
 */
@Configuration
public class MongoIndexConfig {

    private final MongoTemplate mongoTemplate;

    public MongoIndexConfig(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @PostConstruct
    public void initIndexes() {
        createSessionAggregateIndexes();
        createPageMetricIndexes();
    }

    private void createSessionAggregateIndexes() {
        IndexOperations indexOps = mongoTemplate.indexOps(SessionAggregate.class);

        // Compound index: userId + startTime (descending)
        indexOps.ensureIndex(new Index()
                .on("userId", Sort.Direction.ASC)
                .on("startTime", Sort.Direction.DESC)
                .named("user_time_idx"));

        // Single field index: startTime (for time-range queries)
        indexOps.ensureIndex(new Index()
                .on("startTime", Sort.Direction.DESC)
                .named("startTime_idx"));
    }

    private void createPageMetricIndexes() {
        IndexOperations indexOps = mongoTemplate.indexOps(PageMetric.class);

        // Compound index: pageUrl + windowStart (descending)
        indexOps.ensureIndex(new Index()
                .on("pageUrl", Sort.Direction.ASC)
                .on("windowStart", Sort.Direction.DESC)
                .named("page_window_idx"));

        // Single field index: windowStart (for time-range queries)
        indexOps.ensureIndex(new Index()
                .on("windowStart", Sort.Direction.DESC)
                .named("windowStart_idx"));

        // Index for top pages query (totalViews descending)
        indexOps.ensureIndex(new Index()
                .on("totalViews", Sort.Direction.DESC)
                .named("totalViews_idx"));
    }
}
