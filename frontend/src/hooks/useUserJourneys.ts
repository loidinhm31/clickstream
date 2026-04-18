import { useQuery, UseQueryResult } from '@tanstack/react-query';
import { apiClient } from '../services/apiClient';
import { UserJourney, FilterParams } from '../types/analytics';

interface JourneysResponse {
  journeys: UserJourney[];
  total: number;
  page: number;
  limit: number;
}

export function useUserJourneys(
  filters: FilterParams
): UseQueryResult<JourneysResponse> {
  return useQuery({
    queryKey: ['userJourneys', filters],
    queryFn: () =>
      apiClient.get<JourneysResponse>('/api/analytics/journeys', {
        params: filters,
      }),
    staleTime: 30000,
    retry: 2,
  });
}
