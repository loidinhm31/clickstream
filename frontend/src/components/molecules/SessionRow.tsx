import { SessionAggregate } from '../../types/analytics';
import { Badge } from '../atoms/Badge';
import './SessionRow.css';

interface SessionRowProps {
  session: SessionAggregate;
  onClick?: () => void;
}

export function SessionRow({ session, onClick }: SessionRowProps) {
  const duration = session.lastEventTime - session.firstEventTime;
  const durationMinutes = Math.floor(duration / 60000);

  return (
    <div className="session-row" onClick={onClick}>
      <div className="session-col session-id">
        {session.sessionId.substring(0, 8)}...
      </div>
      <div className="session-col session-user">
        {session.userId}
      </div>
      <div className="session-col session-metrics">
        <Badge variant="default">{session.totalClicks} clicks</Badge>
        <Badge variant="success">{session.totalPageViews} views</Badge>
      </div>
      <div className="session-col session-duration">
        {durationMinutes}m
      </div>
      <div className="session-col session-pages">
        {session.uniquePages}
      </div>
    </div>
  );
}
