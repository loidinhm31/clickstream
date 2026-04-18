import './MetricValue.css';

interface MetricValueProps {
  label: string;
  value: number | string;
  unit?: string;
}

export function MetricValue({ label, value, unit }: MetricValueProps) {
  return (
    <div className="metric-value">
      <div className="metric-label">{label}</div>
      <div className="metric-number">
        {value}
        {unit && <span className="metric-unit">{unit}</span>}
      </div>
    </div>
  );
}
