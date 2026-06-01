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
    queryFn: async () => {
      const response = await apiClient.get<any>('/api/analytics/pages', {
        params: {
          ...filters,
          page: (filters.page || 1) - 1, // Spring uses 0-indexed pages
          size: filters.limit || 20,
        },
      });
      
      return {
        metrics: response.content || [],
        total: response.totalElements || 0,
        page: (response.number || 0) + 1,
        limit: response.size || 20,
      };
    },
    staleTime: 30000,
    retry: 2,
  });
}
