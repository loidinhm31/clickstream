import { test, expect } from '@playwright/test';
import { v4 as uuidv4 } from 'uuid';
import { MongoVerifier } from '../utils/MongoVerifier';
import { RealtimeVerifier } from '../utils/RealtimeVerifier';
import { ParquetVerifier } from '../utils/ParquetVerifier';

test.describe('Standard User Journey', () => {
  let mongoVerifier: MongoVerifier;
  const realtimeVerifier = new RealtimeVerifier();
  const parquetVerifier = new ParquetVerifier();
  const testSessionId = uuidv4();

  test.beforeAll(async () => {
    mongoVerifier = new MongoVerifier();
    await mongoVerifier.connect();
  });

  test.afterAll(async () => {
    await mongoVerifier.disconnect();
  });

  test('should track user journey from frontend to all sinks', async ({ page }) => {
    // Capture console logs
    page.on('console', msg => console.log(`BROWSER: ${msg.text()}`));
    
    console.log(`Starting E2E test with Session ID: ${testSessionId}`);

    // 1. Prepare browser context with session ID
    await page.goto('/');
    await page.evaluate(({ sessionId }) => {
      sessionStorage.setItem('clickstream_session_id', sessionId);
      sessionStorage.setItem('clickstream_session_timestamp', Date.now().toString());
    }, { sessionId: testSessionId });

    // Reload to ensure TrackingContext picks up the injected session
    await page.reload();

    // 2. Perform actions on Home Page
    await expect(page).toHaveTitle(/Clickstream/);
    
    // Simulate some scrolling
    await page.mouse.wheel(0, 500);
    await page.waitForTimeout(500);
    await page.mouse.wheel(0, 500);
    await page.waitForTimeout(500);

    // 3. Navigate to a different page
    // Assuming there's a link to "Sessions" or something similar in the NavigationBar
    console.log('Navigating to Sessions page...');
    const navSessionsLink = page.getByRole('link', { name: /Sessions/i });
    if (await navSessionsLink.isVisible()) {
      await navSessionsLink.click();
    } else {
      // Fallback: direct navigation if link not found
      await page.goto('/sessions');
    }
    
    await expect(page).toHaveURL(/.*sessions/);
    await page.waitForTimeout(1000);

    // 4. Trigger a specific click event
    console.log('Clicking "Dashboard" in navigation bar to trigger tracking...');
    const dashboardLink = page.getByRole('link', { name: /Dashboard/i });
    await expect(dashboardLink).toBeVisible();
    
    // We expect a batch request after this click (event tracking service flushes every 2s or 10 events)
    const [response] = await Promise.all([
      page.waitForResponse(res => res.url().includes('/api/events/batch') && res.status() === 202, { timeout: 15000 }),
      dashboardLink.click(),
    ]);
    
    expect(response.status()).toBe(202);
    console.log('Ingestion API accepted the event batch (202 Accepted)');

    // 5. Verify Real-time Analytics
    console.log('Verifying Real-time Analytics stats...');
    const stats = await realtimeVerifier.getStats();
    console.log('Current Real-time Stats:', stats);
    expect(stats).not.toBeNull();
    expect(stats!.activeWebSocketSessions).toBeGreaterThanOrEqual(1);

    // 5.5 Force session close in Spark ETL
    console.log('Waiting for session gap (10s) and sending final events to close session...');
    await page.waitForTimeout(15000); // 15s > 10s gap
    
    // Send multiple clicks to ensure a batch is triggered immediately (limit is 10)
    for (let i = 0; i < 11; i++) {
      await page.click('body');
      await page.waitForTimeout(100);
    }
    
    // Wait for the batch request to complete
    await page.waitForResponse(res => res.url().includes('/api/events/batch'), { timeout: 15000 });
    console.log('Final events flushed to force session aggregation');
    
    // Give Spark ETL a few seconds to process the closing event
    await page.waitForTimeout(5000);

    // 6. Verify MongoDB (Spark ETL)
    console.log('Waiting for Spark ETL to persist session aggregate to MongoDB (timeout 60s)...');
    const sessionDoc = await mongoVerifier.waitForSessionAggregate(testSessionId, 60000);
    expect(sessionDoc).not.toBeNull();
    console.log('Successfully found session aggregate in MongoDB:', sessionDoc.sessionId);
    // Spark ETL writes clickCount / pageViewCount / scrollEvents — no totalEvents field
    const totalEvents = (sessionDoc.clickCount || 0) + (sessionDoc.pageViewCount || 0) + (sessionDoc.scrollEvents || 0);
    expect(totalEvents).toBeGreaterThan(0);

    // 7. Verify Parquet (Raw Archiver)
    console.log('Waiting for Raw Archiver to flush events to Parquet (timeout 90s)...');
    const foundInParquet = await parquetVerifier.waitForSessionInParquet(testSessionId, 90000);
    expect(foundInParquet).toBe(true);
    console.log('Successfully found events in Parquet storage');
  });
});
