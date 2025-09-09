export type JobType = 'batch' | 'streaming'
export type JobLanguage = 'sql' | 'python' | 'scala'
export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled'

export interface JobSchedule {
  cron: string
  timezone?: string
  enabled: boolean
}

export interface SparkResources {
  executors: number
  memory: string
  cores: number
  dynamicAllocation?: boolean
}

export interface SparkJobConfig {
  id: string
  name: string
  type: JobType
  code: string
  language: JobLanguage
  resources: SparkResources
  schedule?: JobSchedule
  dependencies: string[]
  status?: JobStatus
  progress?: number
  output?: any
  logs?: string[]
}