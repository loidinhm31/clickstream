import { MouseEventHandler } from 'react';
import { SessionAggregate } from '../../types/analytics';
import { Badge } from '../atoms/Badge';
import './SessionRow.css';

interface SessionRowProps {
  session: SessionAggregate;
  onClick?: MouseEventHandler<HTMLDivElement>;
}

export function SessionRow({ session, onClick }: SessionRowProps) {
  const durationMinutes = Math.floor(session.durationMs / 60000);
  const durationSeconds = Math.floor((session.durationMs % 60000) / 1000);

  return (
    <div className="session-row" onClick={onClick}>
      <div className="session-col session-id" title={session.sessionId}>
        {session.sessionId.substring(0, 8)}...
      </div>
      <div className="session-col session-user">
        {session.userId}
      </div>
      <div className="session-col session-metrics">
        <Badge variant="default">{session.clickCount} clicks</Badge>
        <Badge variant="success">{session.pageViewCount} views</Badge>
        {session.bounced && <Badge variant="warning">Bounced</Badge>}
      </div>
      <div className="session-col session-duration">
        {durationMinutes}m {durationSeconds}s
      </div>
      <div className="session-col session-pages">
        {session.uniquePages.length} pages
      </div>
    </div>
  );
}
