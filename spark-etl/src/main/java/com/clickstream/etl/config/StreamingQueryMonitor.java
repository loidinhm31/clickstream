package com.clickstream.etl.config;

import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Monitors Spark Structured Streaming query health and metrics.
 * 
 * <p>Logs key performance indicators:
 * <ul>
 *   <li>Processing rate (rows/second)</li>
 *   <li>Batch duration</li>
 *   <li>Input lag (time behind latest data)</li>
 *   <li>Query errors and terminations</li>
 * </ul>
 */
@Component
public class StreamingQueryMonitor extends StreamingQueryListener {
    
    private static final Logger logger = LoggerFactory.getLogger(StreamingQueryMonitor.class);
    
    @Override
    public void onQueryStarted(QueryStartedEvent event) {
        logger.info("Streaming query started: {} (run ID: {})", 
                event.name(), event.runId());
    }
    
    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        var progress = event.progress();
        
        // Log key metrics every 10 batches
        if (progress.batchId() % 10 == 0) {
            long numInputRows = progress.numInputRows();
            double batchDurationMs = progress.durationMs().get("triggerExecution");
            double processRate = numInputRows / Math.max(batchDurationMs / 1000.0, 0.001);
            
            logger.info("Query: {} | Batch: {} | Rows: {} | Duration: {}ms | Rate: {:.2f} rows/sec",
                    progress.name(),
                    progress.batchId(),
                    numInputRows,
                    (long) batchDurationMs,
                    processRate);
            
            // Warn on high lag
            if (progress.sources().length > 0) {
                var source = progress.sources()[0];
                // Check if inputRowsPerSecond and processedRowsPerSecond indicate lag
                double inputRate = source.inputRowsPerSecond();
                double processedRate = source.processedRowsPerSecond();
                if (inputRate > processedRate * 1.5) {
                    logger.warn("Query {} falling behind: input {:.2f} rows/sec > processed {:.2f} rows/sec",
                            progress.name(), inputRate, processedRate);
                }
            }
        }
    }
    
    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {
        if (event.exception().isDefined()) {
            logger.error("Streaming query terminated with error: {} (run ID: {})",
                    event.id(), event.runId(), event.exception().get());
        } else {
            logger.info("Streaming query terminated normally: {} (run ID: {})",
                    event.id(), event.runId());
        }
    }
}
