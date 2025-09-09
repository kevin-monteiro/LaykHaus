import { NextResponse } from 'next/server'

// Use internal Docker network for server-side calls
const LAYKHAUS_API_URL = process.env.LAYKHAUS_INTERNAL_API_URL || 'http://laykhaus-core:8000'

export async function GET() {
  try {
    // Get Kafka connector information
    const connectorsResponse = await fetch(`${LAYKHAUS_API_URL}/api/v1/connectors`)
    
    if (connectorsResponse.ok) {
      const connectorsData = await connectorsResponse.json()
      const kafkaConnectors = connectorsData.connectors.filter((c: any) => c.type === 'kafka')
      
      // Count topics and partitions from Kafka connectors
      let totalTopics = 0
      let totalPartitions = 0
      
      kafkaConnectors.forEach((connector: any) => {
        // Check both config.topics and config.connection.topics
        const topics = connector.config?.topics || connector.config?.connection?.topics || []
        totalTopics += topics.length
        // Assume 3 partitions per topic as default
        totalPartitions += topics.length * 3
      })
      
      return NextResponse.json({
        topics: totalTopics,
        partitions: totalPartitions,
        connectors: kafkaConnectors.length,
        active: kafkaConnectors.filter((c: any) => c.connected).length
      })
    }
  } catch (error) {
    console.error('Error fetching streaming stats:', error)
  }
  
  // Return default values
  return NextResponse.json({
    topics: 0,
    partitions: 0,
    connectors: 0,
    active: 0
  })
}