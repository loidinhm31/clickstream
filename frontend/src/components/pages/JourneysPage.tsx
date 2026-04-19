import { useEffect } from 'react';
import { DashboardTemplate } from '../templates/DashboardTemplate';
import { useUserJourneys } from '../../hooks/useUserJourneys';
import { useClickTracker } from '../../hooks/useClickTracker';
import { Spinner } from '../atoms/Spinner';
import { Badge } from '../atoms/Badge';
import './JourneysPage.css';

export function JourneysPage() {
  const { trackPageView, trackClick } = useClickTracker('JourneysPage');
  const { data, isLoading, error } = useUserJourneys({ page: 1, limit: 20 });

  useEffect(() => {
    trackPageView();
  }, [trackPageView]);

  return (
    <DashboardTemplate>
      <div className="journeys-page">
        <div className="page-header">
          <h2>User Journeys</h2>
          <p>Explore user paths and interaction sequences</p>
        </div>

        {isLoading && (
          <div className="page-loading">
            <Spinner size="lg" />
          </div>
        )}

        {error && (
          <div className="page-error">
            Failed to load user journeys
          </div>
        )}

        {data && (
          <div className="journeys-list">
            {data.journeys.map((journey) => (
              <div
                key={journey.sessionId}
                className="journey-card"
                onClick={() =>
                  trackClick('journey_expand')
                }
              >
                <div className="journey-header">
                  <div className="journey-info">
                    <span className="journey-session">
                      {journey.sessionId.substring(0, 8)}...
                    </span>
                    <Badge variant="default">
                      {journey.totalEvents} events
                    </Badge>
                  </div>
                  <span className="journey-user">{journey.userId}</span>
                </div>

                <div className="journey-events">
                  {journey.eventSequence.map((event) => (
                    <div key={event.sequenceNum} className="journey-event">
                      <span className="event-num">{event.sequenceNum}</span>
                      <Badge
                        variant={
                          event.eventType === 'PAGE_VIEW'
                            ? 'success'
                            : 'default'
                        }
                      >
                        {event.eventType}
                      </Badge>
                      <span className="event-target">
                        {event.targetElement}
                      </span>
                      <span className="event-page">{event.pageUrl}</span>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </DashboardTemplate>
  );
}
