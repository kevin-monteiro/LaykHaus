import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiGet, apiPost } from '@/lib/api/client'
import { QueryInterface, QueryResult } from '@/lib/types/query'

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

export function useExecuteQuery(callbacks?: {
  onSuccess?: (data: QueryResult) => void
  onError?: (title: string, message: string) => void
}) {
  return useMutation({
    mutationFn: async (data: { query: string; dataSources?: string[] }) => {
      // Call core API directly
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/api/v1/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: data.query,
          target_format: 'json',
          max_rows: 100,
        }),
      })
      if (!response.ok) {
        const error = await response.json()
        throw new Error(error.detail || error.error || 'Query execution failed')
      }
      const result = await response.json()
      // Transform to match frontend expectations
      return {
        success: result.success,
        data: result.data || [],
        columns: result.columns || [],
        schema: result.schema || [],
        rowCount: result.row_count || 0,
        executionTime: result.execution_time_ms || 0,
        metadata: result.metadata || {},
      } as QueryResult
    },
    onSuccess: (data, variables) => {
      callbacks?.onSuccess?.(data)
      
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
      const errorMessage = error.response?.data?.detail || error.message || 'An unknown error occurred'
      callbacks?.onError?.('Query execution failed', errorMessage)
      
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

export function useSaveQuery(callbacks?: {
  onSuccess?: (message: string) => void
  onError?: (title: string, message: string) => void
}) {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: (data: Partial<QueryInterface>) =>
      apiPost<QueryInterface>('/api/v1/queries', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['queries'] })
      callbacks?.onSuccess?.('Query saved successfully')
    },
    onError: (error: any) => {
      const errorMessage = error.response?.data?.detail || error.message || 'An unknown error occurred'
      callbacks?.onError?.('Failed to save query', errorMessage)
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
      // Call core API directly to get connectors and their schemas
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
      
      // Get connectors first
      const connectorsResponse = await fetch(`${apiUrl}/api/v1/connectors`)
      if (!connectorsResponse.ok) {
        throw new Error('Failed to fetch connectors')
      }
      const connectorsData = await connectorsResponse.json()
      
      // Transform connectors into schema structure
      const databases = await Promise.all(
        connectorsData.connectors.map(async (connector: any) => {
          let tables = []
          
          try {
            // Get schema for each connector
            const schemaResponse = await fetch(
              `${apiUrl}/api/v1/connectors/${connector.id}/schema`
            )
            
            if (schemaResponse.ok) {
              const schemaData = await schemaResponse.json()
              
              if (schemaData.schema) {
                // Handle REST API endpoints format
                if (schemaData.schema.endpoints) {
                  const endpoints = schemaData.schema.endpoints
                  Object.keys(endpoints).forEach(endpointName => {
                    const endpoint = endpoints[endpointName]
                    tables.push({
                      name: endpointName,
                      columns: endpoint.columns ? Object.keys(endpoint.columns) : []
                    })
                  })
                } 
                // Handle Kafka topics format (topics at root level with fields)
                else if (connector.type === 'kafka') {
                  Object.keys(schemaData.schema).forEach(topicName => {
                    const topicInfo = schemaData.schema[topicName]
                    const columns = topicInfo.fields ? Object.keys(topicInfo.fields) : []
                    tables.push({
                      name: topicName,
                      columns: columns
                    })
                  })
                }
                // Handle PostgreSQL nested schema format
                else {
                  const schemas = Object.keys(schemaData.schema)
                  for (const schemaName of schemas) {
                    const schemaTables = schemaData.schema[schemaName]
                    if (schemaTables && typeof schemaTables === 'object') {
                      Object.keys(schemaTables).forEach(tableName => {
                        const tableInfo = schemaTables[tableName]
                        const columns = tableInfo.columns?.map((col: string) => {
                          try {
                            const parsed = JSON.parse(col)
                            return parsed.column_name
                          } catch {
                            return col
                          }
                        }) || []
                        
                        tables.push({
                          name: `${schemaName}.${tableName}`,
                          columns: columns,
                          row_count: tableInfo.row_count
                        })
                      })
                    }
                  }
                }
              } else {
                tables = schemaData.tables || []
              }
            }
          } catch (error) {
            console.error(`Failed to fetch schema for ${connector.id}:`, error)
          }
          
          // Map based on connector type
          if (connector.type === 'postgresql') {
            return {
              name: 'postgres',
              type: 'postgresql',
              tables: tables
            }
          } else if (connector.type === 'kafka') {
            return {
              name: 'kafka',
              type: 'kafka',
              tables: tables // Use actual schema from backend
            }
          } else if (connector.type === 'rest_api' || connector.type === 'rest') {
            return {
              name: 'rest_api',
              type: 'rest_api',
              tables: tables
            }
          }
          
          return {
            name: connector.id,
            type: connector.type,
            tables: []
          }
        })
      )
      
      return {
        databases: databases.filter(db => db !== null)
      }
    },
    staleTime: 60000,
  })
}