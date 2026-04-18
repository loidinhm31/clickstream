import './StatusDot.css';

interface StatusDotProps {
  connected: boolean;
}

export function StatusDot({ connected }: StatusDotProps) {
  return (
    <div className="status-dot-container">
      <div className={`status-dot ${connected ? 'connected' : 'disconnected'}`} />
      <span className="status-text">
        {connected ? 'Connected' : 'Disconnected'}
      </span>
    </div>
  );
}
