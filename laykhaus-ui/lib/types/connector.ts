export type ConnectorType = 'postgresql' | 'kafka' | 'rest_api' | 'spark' | 'minio'

export type ConnectorStatus = 'active' | 'inactive' | 'error'

export interface AuthConfig {
  type: 'none' | 'basic' | 'bearer' | 'oauth2' | 'api_key'
  credentials?: Record<string, string>
}

export interface SchemaMapping {
  sourceTable: string
  targetTable: string
  fieldMappings: Record<string, string>
}

export interface ConnectorOptions {
  sslMode?: 'disable' | 'require' | 'verify-ca' | 'verify-full'
  connectionTimeout?: number
  maxConnections?: number
  retryAttempts?: number
}

export interface TestResult {
  success: boolean
  message: string
  timestamp: Date
  latency?: number
  details?: Record<string, any>
}

export interface ConnectorConfig {
  id: string
  name: string
  type: ConnectorType | string
  status: ConnectorStatus
  connected?: boolean
  config: {
    connection: Record<string, any>
    authentication: AuthConfig | Record<string, any>
    schema?: SchemaMapping
    options?: ConnectorOptions
    extra_params?: Record<string, any>
  }
  testResults?: TestResult[]
  metadata: {
    createdAt: string | Date
    updatedAt: string | Date
    createdBy?: string
    version?: string
  }
  sql_features?: string[]
  pushdown_capabilities?: string[]
  supports_streaming?: boolean
}