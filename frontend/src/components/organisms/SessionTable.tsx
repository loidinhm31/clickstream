import { useState } from 'react';
import { useSessionAnalytics } from '../../hooks/useSessionAnalytics';
import { useClickTracker } from '../../hooks/useClickTracker';
import { SessionRow } from '../molecules/SessionRow';
import { Button } from '../atoms/Button';
import { Spinner } from '../atoms/Spinner';
import './SessionTable.css';

export function SessionTable() {
  const [page, setPage] = useState(1);
  const { trackClick } = useClickTracker('SessionTable');

  const { data, isLoading, error } = useSessionAnalytics({
    page,
    limit: 20,
  });

  if (isLoading) {
    return (
      <div className="session-table-loading">
        <Spinner size="lg" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="session-table-error">
        Failed to load sessions
      </div>
    );
  }

  return (
    <div className="session-table">
      <div className="session-table-header">
        <div className="header-col">Session ID</div>
        <div className="header-col">User</div>
        <div className="header-col">Activity</div>
        <div className="header-col">Duration</div>
        <div className="header-col">Pages</div>
      </div>

      <div className="session-table-body">
        {data?.sessions.map((session) => (
          <SessionRow
            key={session.sessionId}
            session={session}
            onClick={() =>
              trackClick('session_row', { sessionId: session.sessionId })
            }
          />
        ))}
      </div>

      {data && data.total > 20 && (
        <div className="session-table-footer">
          <Button
            size="sm"
            disabled={page === 1}
            onClick={() => {
              setPage(page - 1);
              trackClick('pagination_prev');
            }}
          >
            Previous
          </Button>
          <span className="page-indicator">
            Page {page} of {Math.ceil(data.total / 20)}
          </span>
          <Button
            size="sm"
            disabled={page * 20 >= data.total}
            onClick={() => {
              setPage(page + 1);
              trackClick('pagination_next');
            }}
          >
            Next
          </Button>
        </div>
      )}
    </div>
  );
}
