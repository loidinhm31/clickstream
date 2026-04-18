import { useQuery, UseQueryResult } from '@tanstack/react-query';
import { apiClient } from '../services/apiClient';
import { PageMetric, FilterParams } from '../types/analytics';

interface PageMetricsResponse {
  metrics: PageMetric[];
  total: number;
  page: number;
  limit: number;
}

export function usePageMetrics(
  filters: FilterParams
): UseQueryResult<PageMetricsResponse> {
  return useQuery({
    queryKey: ['pageMetrics', filters],
    queryFn: () =>
      apiClient.get<PageMetricsResponse>('/api/analytics/pages', {
        params: filters,
      }),
    staleTime: 30000,
    retry: 2,
  });
}
