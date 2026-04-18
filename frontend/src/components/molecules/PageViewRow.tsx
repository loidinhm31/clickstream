import { PageMetric } from '../../types/analytics';
import './PageViewRow.tsx.css';

interface PageViewRowProps {
  metric: PageMetric;
}

export function PageViewRow({ metric }: PageViewRowProps) {
  return (
    <div className="page-view-row">
      <div className="page-col page-url">
        {metric.pageUrl}
      </div>
      <div className="page-col page-views">
        {metric.totalViews.toLocaleString()}
      </div>
      <div className="page-col page-users">
        {metric.uniqueUsers.toLocaleString()}
      </div>
      <div className="page-col page-time">
        {Math.floor(metric.avgTimeOnPage / 1000)}s
      </div>
      <div className="page-col page-bounce">
        {(metric.bounceRate * 100).toFixed(1)}%
      </div>
    </div>
  );
}
