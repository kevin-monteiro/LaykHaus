import { NextResponse } from 'next/server'

export async function GET() {
  try {
    // Try to get Spark UI HTML and parse it
    const sparkUIResponse = await fetch('http://laykhaus-spark-master:8080/')
    
    if (sparkUIResponse.ok) {
      const html = await sparkUIResponse.text()
      
      // Parse HTML to find running/completed apps
      const runningMatch = html.match(/(\d+)\s+<a[^>]*>Running<\/a>/)
      const completedMatch = html.match(/(\d+)\s+<a[^>]*>Completed<\/a>/)
      
      const running = runningMatch ? parseInt(runningMatch[1]) : 0
      const completed = completedMatch ? parseInt(completedMatch[1]) : 0
      
      return NextResponse.json({
        running,
        completed,
        total: running + completed,
        status: running > 0 ? 'active' : 'idle'
      })
    }
  } catch (error) {
    console.error('Error fetching Spark stats:', error)
  }
  
  // Try to get from LaykHaus core API
  try {
    const coreResponse = await fetch('http://laykhaus-core:8000/api/v1/spark/stats')
    if (coreResponse.ok) {
      const data = await coreResponse.json()
      return NextResponse.json(data)
    }
  } catch (error) {
    // Ignore error, return defaults
  }
  
  // Return default values if Spark UI is not accessible
  return NextResponse.json({
    running: 0,
    completed: 0,
    total: 0,
    status: 'idle'
  })
}