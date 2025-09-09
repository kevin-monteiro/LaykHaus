import { NextResponse } from 'next/server'

// Use internal Docker network for server-side calls
const LAYKHAUS_API_URL = process.env.LAYKHAUS_INTERNAL_API_URL || 'http://laykhaus-core:8000'

export async function POST(request: Request) {
  try {
    const body = await request.json()
    
    // Forward to LaykHaus core /api/v1/query endpoint
    const response = await fetch(`${LAYKHAUS_API_URL}/api/v1/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: body.query,
        // Add any additional parameters if needed
      }),
    })

    const data = await response.json()
    
    if (!response.ok) {
      return NextResponse.json(
        { error: data.detail || 'Query execution failed' },
        { status: response.status }
      )
    }

    // Transform response to match frontend expectations
    return NextResponse.json({
      success: data.success,
      data: data.data || [],
      schema: data.schema || [],
      rowCount: data.row_count || 0,
      executionTime: data.execution_time_ms || 0,
      metadata: data.metadata || {},
    })
  } catch (error) {
    console.error('Error executing query:', error)
    return NextResponse.json(
      { error: 'Failed to execute query' },
      { status: 500 }
    )
  }
}