import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiGet, apiPost } from '@/lib/api/client'
import { QueryInterface, QueryResult } from '@/lib/types/query'
import toast from 'react-hot-toast'

export function useQueries() {
  return useQuery({
    queryKey: ['queries'],
    queryFn: () => apiGet<QueryInterface[]>('/api/v1/queries'),
  })
}

export function useQueryById(id: string) {
  return useQuery({
    queryKey: ['queries', id],
    queryFn: () => apiGet<QueryInterface>(`/api/v1/queries/${id}`),
    enabled: !!id,
  })
}

export function useExecuteQuery() {
  return useMutation({
    mutationFn: async (data: { query: string; dataSources?: string[] }) => {
      // Use Next.js API route instead of direct core API
      const response = await fetch('/api/queries/execute', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(data),
      })
      if (!response.ok) {
        const error = await response.json()
        throw new Error(error.error || 'Query execution failed')
      }
      return response.json() as Promise<QueryResult>
    },
    onSuccess: (data, variables) => {
      toast.success(`Query executed successfully (${data.rowCount} rows)`)
      
      // Save to localStorage history
      try {
        const stored = localStorage.getItem('queryHistory')
        const history = stored ? JSON.parse(stored) : []
        
        const newEntry: QueryInterface = {
          id: Date.now().toString(),
          name: `Query ${new Date().toLocaleString()}`,
          type: 'sql',
          query: variables.query,
          dataSources: variables.dataSources || [],
          executionHistory: [{
            id: Date.now().toString(),
            timestamp: new Date(),
            status: 'success',
            duration: data.executionTime,
          }],
        }
        
        // Keep only last 20 queries
        const updatedHistory = [newEntry, ...history].slice(0, 20)
        localStorage.setItem('queryHistory', JSON.stringify(updatedHistory))
        
        // Trigger storage event to update other components
        window.dispatchEvent(new Event('storage'))
      } catch (e) {
        console.error('Failed to save query to history:', e)
      }
    },
    onError: (error: any, variables) => {
      toast.error(error.message || 'Query execution failed')
      
      // Save failed query to history too
      try {
        const stored = localStorage.getItem('queryHistory')
        const history = stored ? JSON.parse(stored) : []
        
        const newEntry: QueryInterface = {
          id: Date.now().toString(),
          name: `Failed Query ${new Date().toLocaleString()}`,
          type: 'sql',
          query: variables.query,
          dataSources: variables.dataSources || [],
          executionHistory: [{
            id: Date.now().toString(),
            timestamp: new Date(),
            status: 'failure',
            error: error.message,
          }],
        }
        
        const updatedHistory = [newEntry, ...history].slice(0, 20)
        localStorage.setItem('queryHistory', JSON.stringify(updatedHistory))
        window.dispatchEvent(new Event('storage'))
      } catch (e) {
        console.error('Failed to save query to history:', e)
      }
    },
  })
}

export function useSaveQuery() {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: (data: Partial<QueryInterface>) =>
      apiPost<QueryInterface>('/api/v1/queries', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['queries'] })
      toast.success('Query saved successfully')
    },
    onError: (error: any) => {
      toast.error(error.message || 'Failed to save query')
    },
  })
}

export function useQueryHistory(queryId?: string) {
  return useQuery({
    queryKey: ['query-history', queryId],
    queryFn: () => 
      queryId 
        ? apiGet<QueryInterface[]>(`/api/v1/queries/${queryId}/history`)
        : apiGet<QueryInterface[]>('/api/v1/queries/history'),
  })
}

export function useSchemas() {
  return useQuery({
    queryKey: ['schemas'],
    queryFn: async () => {
      // Use Next.js API route instead of direct core API
      const response = await fetch('/api/catalog/schemas')
      if (!response.ok) {
        throw new Error('Failed to fetch schemas')
      }
      return response.json()
    },
    staleTime: 60000,
  })
}