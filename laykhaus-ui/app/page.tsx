'use client'

import Link from 'next/link'
import { useEffect, useState } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Header } from '@/components/layout/Header'
import { 
  Database, 
  ArrowRight,
  Globe
} from 'lucide-react'

export default function HomePage() {
  const [stats, setStats] = useState([
    { label: 'Active Connectors', value: '0', change: 'Loading...' },
    { label: 'Queries Today', value: '0', change: 'Loading...' },
    { label: 'Streaming Topics', value: '0', change: 'Loading...' },
    { label: 'Spark Jobs', value: '0', change: 'Loading...' },
  ])

  useEffect(() => {
    // Fetch real stats
    const fetchStats = async () => {
      try {
        // Fetch connectors count
        const connectorsRes = await fetch('/api/connectors/stats')
        if (connectorsRes.ok) {
          const connectorsData = await connectorsRes.json()
          setStats(prev => prev.map(stat => 
            stat.label === 'Active Connectors' 
              ? { ...stat, value: String(connectorsData.total || 0), change: `${connectorsData.active || 0} active` }
              : stat
          ))
        }

        // Fetch Spark jobs info
        const sparkRes = await fetch('/api/spark/stats')
        if (sparkRes.ok) {
          const sparkData = await sparkRes.json()
          setStats(prev => prev.map(stat => 
            stat.label === 'Spark Jobs' 
              ? { ...stat, value: String(sparkData.running || 0), change: sparkData.running > 0 ? 'Running' : 'Idle' }
              : stat
          ))
        }

        // Fetch query stats
        const queryRes = await fetch('/api/queries/stats')
        if (queryRes.ok) {
          const queryData = await queryRes.json()
          setStats(prev => prev.map(stat => 
            stat.label === 'Queries Today' 
              ? { ...stat, value: String(queryData.today || 0), change: `${queryData.total || 0} total` }
              : stat
          ))
        }

        // Fetch streaming stats
        const streamingRes = await fetch('/api/streaming/stats')
        if (streamingRes.ok) {
          const streamingData = await streamingRes.json()
          setStats(prev => prev.map(stat => 
            stat.label === 'Streaming Topics' 
              ? { ...stat, value: String(streamingData.topics || 0), change: `${streamingData.partitions || 0} partitions` }
              : stat
          ))
        }
      } catch (error) {
        console.error('Error fetching stats:', error)
      }
    }

    fetchStats()
    // Refresh stats every 30 seconds
    const interval = setInterval(fetchStats, 30000)
    return () => clearInterval(interval)
  }, [])
  const features = [
    {
      title: 'Connector Management',
      description: 'Configure and manage data source connections',
      icon: Database,
      href: '/connectors',
      color: 'text-blue-600',
      bgColor: 'bg-blue-50',
    },
    {
      title: 'Query Builder',
      description: 'Build and execute federated queries',
      icon: Globe,
      href: '/query',
      color: 'text-green-600',
      bgColor: 'bg-green-50',
    },
  ]

  return (
    <div className="min-h-screen bg-gradient-to-b from-gray-50 to-white dark:from-gray-900 dark:to-gray-800" data-test-id="home-page">
      <Header />
      <div className="container mx-auto px-4 py-8">
        {/* Hero Section */}
        <div className="mb-8 text-center" data-test-id="hero-section">
          <p className="text-lg text-gray-600 dark:text-gray-400" data-test-id="tagline">
            User interface for federated data LaykHaus platform
          </p>
        </div>

        {/* Stats Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8" data-test-id="stats-grid">
          {stats.map((stat) => {
            // Make Spark Jobs card clickable
            const isSparkCard = stat.label === 'Spark Jobs'
            const CardWrapper = isSparkCard ? 'a' : 'div'
            const cardProps = isSparkCard ? { 
              href: 'http://localhost:8081', 
              target: '_blank',
              rel: 'noopener noreferrer',
              className: 'block'
            } : {}
            
            return (
              <CardWrapper key={stat.label} {...cardProps} data-test-id={`stat-card-${stat.label.toLowerCase().replace(/\s+/g, '-')}`}>
                <Card className={isSparkCard ? 'hover:shadow-lg transition-shadow cursor-pointer' : ''}>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm text-gray-600 dark:text-gray-400">
                          {stat.label}
                          {isSparkCard && (
                            <span className="ml-1 text-xs">↗</span>
                          )}
                        </p>
                        <p className="text-2xl font-bold mt-1">{stat.value}</p>
                      </div>
                      <div className="text-sm font-medium text-green-600 dark:text-green-400">
                        {stat.change}
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </CardWrapper>
            )
          })}
        </div>

        {/* Features Grid - Optimized for better real estate usage */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 max-w-6xl mx-auto" data-test-id="features-grid">
          {features.map((feature) => {
            const Icon = feature.icon
            return (
              <Link key={feature.title} href={feature.href} className="group" data-test-id={`feature-card-${feature.title.toLowerCase().replace(/\s+/g, '-')}`}>
                <Card className="h-full hover:shadow-xl transition-all duration-300 cursor-pointer hover:scale-[1.02] group-hover:border-blue-200 dark:group-hover:border-blue-800">
                  <CardHeader className="p-8">
                    <div className="flex items-center justify-between mb-6">
                      <div className={`p-4 rounded-xl ${feature.bgColor}`}>
                        <Icon className={`h-8 w-8 ${feature.color}`} />
                      </div>
                      <ArrowRight className="h-6 w-6 text-gray-400 group-hover:translate-x-1 transition-transform" />
                    </div>
                    <CardTitle className="text-2xl mb-3">{feature.title}</CardTitle>
                    <CardDescription className="text-base">{feature.description}</CardDescription>
                  </CardHeader>
                </Card>
              </Link>
            )
          })}
        </div>
      </div>
    </div>
  )
}