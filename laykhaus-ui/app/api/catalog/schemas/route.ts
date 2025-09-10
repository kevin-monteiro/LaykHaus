import { NextResponse } from 'next/server'

// Use internal Docker network for server-side calls
const LAYKHAUS_API_URL = process.env.LAYKHAUS_INTERNAL_API_URL || 'http://laykhaus-core:8000'

export async function GET() {
  try {
    // Get list of connectors first
    const connectorsResponse = await fetch(`${LAYKHAUS_API_URL}/api/v1/connectors`, {
      cache: 'no-store',
      headers: {
        'Content-Type': 'application/json',
      }
    })
    const connectorsData = await connectorsResponse.json()
    
    if (!connectorsResponse.ok) {
      throw new Error('Failed to fetch connectors')
    }

    // Transform connectors into schema structure
    const databases = await Promise.all(
      connectorsData.connectors.map(async (connector: any) => {
        // Get schema for each connector
        let tables = []
        
        try {
          const schemaResponse = await fetch(
            `${LAYKHAUS_API_URL}/api/v1/connectors/${connector.id}/schema`
          )
          
          if (schemaResponse.ok) {
            const schemaData = await schemaResponse.json()
            // Parse the nested schema structure
            if (schemaData.schema) {
              // Check if it's REST API endpoints format
              if (schemaData.schema.endpoints) {
                // Handle REST API endpoints
                const endpoints = schemaData.schema.endpoints
                Object.keys(endpoints).forEach(endpointName => {
                  const endpoint = endpoints[endpointName]
                  const columns = endpoint.columns 
                    ? Object.keys(endpoint.columns) 
                    : []
                  
                  tables.push({
                    name: endpointName,
                    columns: columns
                  })
                })
              } 
              // For PostgreSQL, schema is nested by schema name (e.g., "solar")
              else {
                const schemas = Object.keys(schemaData.schema)
                for (const schemaName of schemas) {
                  const schemaTables = schemaData.schema[schemaName]
                  
                  // Handle PostgreSQL tables object
                  if (schemaTables && typeof schemaTables === 'object') {
                    Object.keys(schemaTables).forEach(tableName => {
                      const tableInfo = schemaTables[tableName]
                      // Parse column information
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
            name: 'postgres',  // Always use 'postgres' for PostgreSQL connectors
            type: 'postgresql',
            tables: tables // Use the actual tables we fetched
          }
        } else if (connector.type === 'kafka') {
          // Get topics from connection config
          const topics = connector.config?.connection?.topics || connector.config?.topics || []
          return {
            name: 'kafka',  // Always use 'kafka' for Kafka connectors
            type: 'kafka',
            tables: topics.map((topic: string) => ({
              name: topic,
              columns: ['key', 'value', 'timestamp', 'partition', 'offset']
            }))
          }
        } else if (connector.type === 'rest_api' || connector.type === 'rest') {
          return {
            name: 'rest_api',  // Always use 'rest_api' for REST API connectors
            type: 'rest_api',
            tables: tables // Use actual schema, no fallback
          }
        }
        
        return {
          name: connector.id,
          type: connector.type,
          tables: []
        }
      })
    )

    return NextResponse.json({
      databases: databases.filter(db => db !== null)
    })
  } catch (error) {
    console.error('Error fetching schemas:', error)
    return NextResponse.json(
      { 
        databases: [],
        error: 'Failed to fetch schemas' 
      },
      { status: 500 }
    )
  }
}