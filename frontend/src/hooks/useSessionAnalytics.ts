import { useQuery, UseQueryResult } from '@tanstack/react-query';
import { apiClient } from '../services/apiClient';
import { SessionAggregate, FilterParams } from '../types/analytics';

interface SessionsResponse {
  sessions: SessionAggregate[];
  total: number;
  page: number;
  limit: number;
}

export function useSessionAnalytics(
  filters: FilterParams
): UseQueryResult<SessionsResponse> {
  return useQuery({
    queryKey: ['sessions', filters],
    queryFn: () =>
      apiClient.get<SessionsResponse>('/api/analytics/sessions', {
        params: filters,
      }),
    staleTime: 30000, // 30s cache for historical data
    retry: 2,
  });
}
