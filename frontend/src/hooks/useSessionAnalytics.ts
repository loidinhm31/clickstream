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
    queryFn: async () => {
      const response = await apiClient.get<any>('/api/analytics/sessions', {
        params: {
          ...filters,
          page: (filters.page || 1) - 1, // Spring uses 0-indexed pages
          size: filters.limit || 20,
        },
      });
      
      // Map Spring Data Page to SessionsResponse
      return {
        sessions: response.content || [],
        total: response.totalElements || 0,
        page: (response.number || 0) + 1,
        limit: response.size || 20,
      };
    },
    staleTime: 30000, // 30s cache for historical data
    retry: 2,
  });
}
