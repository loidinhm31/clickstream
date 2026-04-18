import { MetricValue } from '../atoms/MetricValue';
import { Sparkline } from '../atoms/Sparkline';
import './MetricCard.css';

interface MetricCardProps {
  label: string;
  value: number | string;
  unit?: string;
  trend?: number[]; // Historical data for sparkline
  changePercent?: number; // Increase/decrease percentage
}

export function MetricCard({ 
  label, 
  value, 
  unit, 
  trend, 
  changePercent 
}: MetricCardProps) {
  const isPositive = changePercent && changePercent > 0;

  return (
    <div className="metric-card">
      <div className="metric-card-header">
        <MetricValue label={label} value={value} unit={unit} />
        {changePercent !== undefined && (
          <div className={`metric-change ${isPositive ? 'positive' : 'negative'}`}>
            <span className="change-arrow">{isPositive ? '↑' : '↓'}</span>
            {Math.abs(changePercent).toFixed(1)}%
          </div>
        )}
      </div>
      {trend && trend.length > 0 && (
        <div className="metric-card-chart">
          <Sparkline data={trend} color={isPositive ? '#10b981' : '#ef4444'} />
        </div>
      )}
    </div>
  );
}
