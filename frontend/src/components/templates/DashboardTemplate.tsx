import { ReactNode } from 'react';
import { NavigationBar } from '../organisms/NavigationBar';
import './DashboardTemplate.css';

interface DashboardTemplateProps {
  children: ReactNode;
}

export function DashboardTemplate({ children }: DashboardTemplateProps) {
  return (
    <div className="dashboard-template">
      <NavigationBar />
      <main className="dashboard-content">{children}</main>
    </div>
  );
}
