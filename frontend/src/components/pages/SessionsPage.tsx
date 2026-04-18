import { useEffect } from 'react';
import { DashboardTemplate } from '../templates/DashboardTemplate';
import { SessionTable } from '../organisms/SessionTable';
import { useClickTracker } from '../../hooks/useClickTracker';
import './SessionsPage.css';

export function SessionsPage() {
  const { trackPageView } = useClickTracker('SessionsPage');

  useEffect(() => {
    trackPageView();
  }, [trackPageView]);

  return (
    <DashboardTemplate>
      <div className="sessions-page">
        <div className="page-header">
          <h2>Session Analytics</h2>
          <p>View detailed session data and user activity patterns</p>
        </div>
        <SessionTable />
      </div>
    </DashboardTemplate>
  );
}
