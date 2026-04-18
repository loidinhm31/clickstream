import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { TrackingProvider } from './contexts/TrackingContext';
import { RealtimeProvider } from './contexts/RealtimeContext';
import { ErrorBoundary } from './components/ErrorBoundary';
import { DashboardPage } from './components/pages/DashboardPage';
import { SessionsPage } from './components/pages/SessionsPage';
import { PagesAnalyticsPage } from './components/pages/PagesAnalyticsPage';
import { JourneysPage } from './components/pages/JourneysPage';
import './App.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 2,
    },
  },
});

function App() {
  return (
    <ErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <TrackingProvider userId="anonymous">
          <RealtimeProvider>
            <BrowserRouter>
              <Routes>
                <Route path="/" element={<DashboardPage />} />
                <Route path="/sessions" element={<SessionsPage />} />
                <Route path="/pages" element={<PagesAnalyticsPage />} />
                <Route path="/journeys" element={<JourneysPage />} />
              </Routes>
            </BrowserRouter>
          </RealtimeProvider>
        </TrackingProvider>
      </QueryClientProvider>
    </ErrorBoundary>
  );
}

export default App;
