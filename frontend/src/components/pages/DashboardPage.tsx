import { useEffect } from 'react';
import { DashboardTemplate } from '../templates/DashboardTemplate';
import { RealtimeDashboard } from '../organisms/RealtimeDashboard';
import { useClickTracker } from '../../hooks/useClickTracker';

export function DashboardPage() {
  const { trackPageView } = useClickTracker('DashboardPage');

  useEffect(() => {
    trackPageView();
  }, [trackPageView]);

  return (
    <DashboardTemplate>
      <RealtimeDashboard />
    </DashboardTemplate>
  );
}
