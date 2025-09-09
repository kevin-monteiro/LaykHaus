export interface KafkaTopic {
  name: string
  partitions: number
  replicationFactor: number
  config?: Record<string, any>
}

export interface ConsumerGroup {
  id: string
  topics: string[]
  status: 'active' | 'paused' | 'stopped'
  lag?: number
  members?: string[]
}

export interface ProducerConfig {
  id: string
  topic: string
  format: 'json' | 'avro' | 'protobuf'
  compressionType?: 'none' | 'gzip' | 'snappy' | 'lz4'
}

export interface StreamProcessor {
  id: string
  name: string
  source: string
  sink: string
  transformations: Array<{
    type: 'filter' | 'map' | 'aggregate' | 'join'
    config: Record<string, any>
  }>
}

export interface StreamingMetrics {
  lag: number
  throughput: number
  errors: Array<{
    timestamp: Date
    message: string
    topic?: string
  }>
}

export interface StreamingConfig {
  kafkaTopics: KafkaTopic[]
  consumers: ConsumerGroup[]
  producers: ProducerConfig[]
  streamProcessors: StreamProcessor[]
  monitoring: StreamingMetrics
}