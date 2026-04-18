import { useEffect } from 'react';
import { DashboardTemplate } from '../templates/DashboardTemplate';
import { PageViewRow } from '../molecules/PageViewRow';
import { usePageMetrics } from '../../hooks/usePageMetrics';
import { useClickTracker } from '../../hooks/useClickTracker';
import { Spinner } from '../atoms/Spinner';
import './PagesAnalyticsPage.css';

export function PagesAnalyticsPage() {
  const { trackPageView } = useClickTracker('PagesAnalyticsPage');
  const { data, isLoading, error } = usePageMetrics({ page: 1, limit: 50 });

  useEffect(() => {
    trackPageView();
  }, [trackPageView]);

  return (
    <DashboardTemplate>
      <div className="pages-analytics-page">
        <div className="page-header">
          <h2>Page Metrics</h2>
          <p>Analyze page views, bounce rates, and user engagement</p>
        </div>

        {isLoading && (
          <div className="page-loading">
            <Spinner size="lg" />
          </div>
        )}

        {error && (
          <div className="page-error">
            Failed to load page metrics
          </div>
        )}

        {data && (
          <div className="metrics-table">
            <div className="metrics-table-header">
              <div className="header-col">Page URL</div>
              <div className="header-col">Total Views</div>
              <div className="header-col">Unique Users</div>
              <div className="header-col">Avg Time</div>
              <div className="header-col">Bounce Rate</div>
            </div>
            <div className="metrics-table-body">
              {data.metrics.map((metric, index) => (
                <PageViewRow key={index} metric={metric} />
              ))}
            </div>
          </div>
        )}
      </div>
    </DashboardTemplate>
  );
}
