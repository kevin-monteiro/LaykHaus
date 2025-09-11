import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiGet, apiPost, apiPut, apiDelete } from '@/lib/api/client'
import { ConnectorConfig } from '@/lib/types/connector'

export function useConnectors() {
  return useQuery({
    queryKey: ['connectors'],
    queryFn: async () => {
      const response = await apiGet<{connectors: ConnectorConfig[]}>('/api/v1/connectors')
      return response.connectors || []
    },
    staleTime: 30000,
  })
}

export function useConnectorStats() {
  return useQuery({
    queryKey: ['connector-stats'],
    queryFn: () => apiGet<{
      total: number
      active: number
      inactive: number
      error: number
      by_type: {
        postgresql: number
        kafka: number
        rest_api: number
      }
    }>('/api/v1/connectors/stats'),
    staleTime: 30000,
  })
}

export function useConnector(id: string) {
  return useQuery({
    queryKey: ['connectors', id],
    queryFn: () => apiGet<ConnectorConfig>(`/api/v1/connectors/${id}`),
    enabled: !!id,
  })
}

export function useCreateConnector(callbacks?: {
  onSuccess?: (message: string) => void
  onError?: (title: string, message: string) => void
}) {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: (data: Partial<ConnectorConfig>) => 
      apiPost<ConnectorConfig>('/api/v1/connectors', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['connectors'] })
      queryClient.invalidateQueries({ queryKey: ['connector-stats'] })
      callbacks?.onSuccess?.('Connector created successfully')
    },
    onError: (error: any) => {
      const errorMessage = error.response?.data?.detail || error.message || 'An unknown error occurred'
      callbacks?.onError?.('Failed to create connector', errorMessage)
    },
  })
}

export function useUpdateConnector(callbacks?: {
  onSuccess?: (message: string) => void
  onError?: (title: string, message: string) => void
}) {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<ConnectorConfig> }) =>
      apiPut<ConnectorConfig>(`/api/v1/connectors/${id}`, data),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['connectors'] })
      queryClient.invalidateQueries({ queryKey: ['connectors', variables.id] })
      queryClient.invalidateQueries({ queryKey: ['connector-stats'] })
      callbacks?.onSuccess?.('Connector updated successfully')
    },
    onError: (error: any) => {
      const errorMessage = error.response?.data?.detail || error.message || 'An unknown error occurred'
      callbacks?.onError?.('Failed to update connector', errorMessage)
    },
  })
}

export function useDeleteConnector(callbacks?: {
  onSuccess?: (message: string) => void
  onError?: (title: string, message: string) => void
}) {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: (id: string) => apiDelete(`/api/v1/connectors/${id}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['connectors'] })
      queryClient.invalidateQueries({ queryKey: ['connector-stats'] })
      callbacks?.onSuccess?.('Connector deleted successfully')
    },
    onError: (error: any) => {
      const errorMessage = error.response?.data?.detail || error.message || 'An unknown error occurred'
      callbacks?.onError?.('Failed to delete connector', errorMessage)
    },
  })
}

export function useTestConnector(callbacks?: {
  onSuccess?: (message: string) => void
  onError?: (title: string, message: string) => void
}) {
  return useMutation({
    mutationFn: (id: string) => 
      apiPost<{ 
        connected: boolean; 
        health: { 
          status: string; 
          message: string; 
          details: any 
        } 
      }>(`/api/v1/connectors/${id}/test`),
    onSuccess: (data) => {
      if (data.connected && data.health.status === 'healthy') {
        callbacks?.onSuccess?.(`Connection test successful. ${data.health.message}`)
      } else {
        const errorMsg = data.health.message || 'Connection test failed'
        callbacks?.onError?.('Connection test failed', errorMsg)
      }
    },
    onError: (error: any) => {
      const errorMessage = error.response?.data?.detail || error.message || 'An unknown error occurred'
      callbacks?.onError?.('Connection test failed', errorMessage)
    },
  })
}