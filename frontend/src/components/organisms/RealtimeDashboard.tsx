import { useEffect } from 'react';
import { useRealtimeMetrics } from '../../hooks/useRealtimeMetrics';
import { useClickTracker } from '../../hooks/useClickTracker';
import { MetricCard } from '../molecules/MetricCard';
import { StatusDot } from '../atoms/StatusDot';
import { Spinner } from '../atoms/Spinner';
import { sanitizeUrl } from '../../utils/security';
import './RealtimeDashboard.css';

export function RealtimeDashboard() {
  const { metrics, connected, error } = useRealtimeMetrics();
  const { trackEvent } = useClickTracker('RealtimeDashboard');

  useEffect(() => {
    trackEvent('PAGE_VIEW', 'dashboard_view');
  }, [trackEvent]);

  if (error) {
    return (
      <div className="realtime-error">
        <p>Failed to load metrics: {error}</p>
      </div>
    );
  }

  if (!metrics) {
    return (
      <div className="realtime-loading">
        <Spinner size="lg" />
        <p>Loading real-time metrics...</p>
      </div>
    );
  }

  return (
    <div className="realtime-dashboard">
      <div className="dashboard-header">
        <h2>Real-time Analytics</h2>
        <StatusDot connected={connected} />
      </div>

      <div className="metrics-grid">
        <MetricCard
          label="Active Users"
          value={metrics.activeUsers}
          changePercent={5.2}
        />
        <MetricCard
          label="Clicks per Second"
          value={metrics.clicksPerSecond.toFixed(2)}
          changePercent={-2.1}
        />
        <MetricCard
          label="Event Rate"
          value={metrics.eventRate.toFixed(1)}
          unit="/s"
          changePercent={3.8}
        />
      </div>

      {metrics.trendingPages.length > 0 && (
        <div className="trending-section">
          <h3>Trending Pages</h3>
          <div className="trending-list">
            {metrics.trendingPages.map((page, index) => (
              <div
                key={index}
                className="trending-item"
                onClick={() =>
                  trackEvent('CLICK', 'trending_page', { url: page.url })
                }
              >
                <span className="trending-rank">#{index + 1}</span>
                <span className="trending-url">{sanitizeUrl(page.url)}</span>
                <span className="trending-views">{page.views} views</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
