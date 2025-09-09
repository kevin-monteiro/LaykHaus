import { NextResponse } from 'next/server'

// Use internal Docker network for server-side calls
const LAYKHAUS_API_URL = process.env.LAYKHAUS_INTERNAL_API_URL || 'http://laykhaus-core:8000'

export async function GET() {
  try {
    // Get list of connectors
    const connectorsResponse = await fetch(`${LAYKHAUS_API_URL}/api/v1/connectors`)
    const connectorsData = await connectorsResponse.json()
    
    if (!connectorsResponse.ok) {
      throw new Error('Failed to fetch connectors')
    }

    // Count active vs inactive connectors
    const connectors = connectorsData.connectors || []
    const active = connectors.filter((c: any) => c.connected).length
    const total = connectors.length

    return NextResponse.json({
      total,
      active,
      inactive: total - active,
      error: 0
    })
  } catch (error) {
    console.error('Error fetching connector stats:', error)
    return NextResponse.json({
      total: 0,
      active: 0,
      inactive: 0,
      error: 0
    })
  }
}