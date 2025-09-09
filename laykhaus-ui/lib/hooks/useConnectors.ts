import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiGet, apiPost, apiPut, apiDelete } from '@/lib/api/client'
import { ConnectorConfig } from '@/lib/types/connector'
import toast from 'react-hot-toast'

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

export function useCreateConnector() {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: (data: Partial<ConnectorConfig>) => 
      apiPost<ConnectorConfig>('/api/v1/connectors', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['connectors'] })
      toast.success('Connector created successfully')
    },
    onError: (error: any) => {
      toast.error(error.message || 'Failed to create connector')
    },
  })
}

export function useUpdateConnector() {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<ConnectorConfig> }) =>
      apiPut<ConnectorConfig>(`/api/v1/connectors/${id}`, data),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['connectors'] })
      queryClient.invalidateQueries({ queryKey: ['connectors', variables.id] })
      toast.success('Connector updated successfully')
    },
    onError: (error: any) => {
      toast.error(error.message || 'Failed to update connector')
    },
  })
}

export function useDeleteConnector() {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: (id: string) => apiDelete(`/api/v1/connectors/${id}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['connectors'] })
      toast.success('Connector deleted successfully')
    },
    onError: (error: any) => {
      toast.error(error.message || 'Failed to delete connector')
    },
  })
}

export function useTestConnector() {
  return useMutation({
    mutationFn: (id: string) => 
      apiPost<{ success: boolean; message: string }>(`/api/v1/connectors/${id}/test`),
    onSuccess: (data) => {
      if (data.success) {
        toast.success('Connection test successful')
      } else {
        toast.error(data.message || 'Connection test failed')
      }
    },
    onError: (error: any) => {
      toast.error(error.message || 'Connection test failed')
    },
  })
}