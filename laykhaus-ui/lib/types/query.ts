export type QueryType = 'sql' | 'graphql' | 'federated'

export interface QueryParameter {
  name: string
  type: 'string' | 'number' | 'boolean' | 'date'
  value?: any
  required?: boolean
}

export interface CronSchedule {
  expression: string
  timezone?: string
  enabled: boolean
}

export interface QueryResult {
  data: any[]
  columns: string[]
  rowCount: number
  executionTime: number
  error?: string
}

export interface ExecutionLog {
  id: string
  timestamp: Date
  status: 'success' | 'failure' | 'running'
  duration?: number
  error?: string
}

export interface QueryInterface {
  id: string
  name: string
  type: QueryType
  query: string
  dataSources: string[]
  parameters?: QueryParameter[]
  schedule?: CronSchedule
  results?: QueryResult
  executionHistory: ExecutionLog[]
}