import { NextResponse } from 'next/server'

// Use internal Docker network for server-side calls
const LAYKHAUS_API_URL = process.env.LAYKHAUS_INTERNAL_API_URL || 'http://laykhaus-core:8000'

export async function POST(
  request: Request,
  { params }: { params: { id: string } }
) {
  try {
    const response = await fetch(`${LAYKHAUS_API_URL}/api/v1/connectors/${params.id}/test`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    })

    const data = await response.json()
    return NextResponse.json(data)
  } catch (error) {
    console.error('Error testing connector:', error)
    return NextResponse.json(
      { error: 'Failed to test connector' },
      { status: 500 }
    )
  }
}