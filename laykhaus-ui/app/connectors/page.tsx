'use client'

import { useState } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Header } from '@/components/layout/Header'
import { ConnectorCard } from '@/components/connectors/ConnectorCard'
import { ConnectorDialog } from '@/components/connectors/ConnectorDialog'
import { Plus, Search, Filter, RefreshCw } from 'lucide-react'
import { useConnectors, useConnectorStats, useDeleteConnector } from '@/lib/hooks/useConnectors'
import toast from 'react-hot-toast'

export default function ConnectorsPage() {
  const [searchQuery, setSearchQuery] = useState('')
  const [isDialogOpen, setIsDialogOpen] = useState(false)
  const [selectedConnector, setSelectedConnector] = useState<any>(null)
  
  const { data: connectors, isLoading, refetch } = useConnectors()
  const { data: stats } = useConnectorStats()
  const deleteConnector = useDeleteConnector()

  const filteredConnectors = connectors?.filter(connector =>
    connector.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
    connector.type.toLowerCase().includes(searchQuery.toLowerCase())
  ) || []
  
  const handleDelete = async (connectorId: string) => {
    if (confirm('Are you sure you want to delete this connector?')) {
      try {
        await deleteConnector.mutateAsync(connectorId)
        refetch()
      } catch (error) {
        // Error handled by mutation
      }
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-b from-gray-50 to-white dark:from-gray-900 dark:to-gray-800" data-test-id="connectors-page">
      <Header />
      <div className="container mx-auto px-4 py-8">
        {/* Page Description */}
        <div className="mb-8 text-center" data-test-id="page-description">
          <p className="text-gray-600 dark:text-gray-400" data-test-id="page-tagline">
            Configure and manage your data source connections
          </p>
        </div>

      {/* Actions Bar */}
      <div className="flex flex-col sm:flex-row gap-4 mb-6" data-test-id="actions-bar">
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
          <Input
            placeholder="Search connectors..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10"
            data-test-id="search-input"
          />
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={() => refetch()} data-test-id="refresh-button">
            <RefreshCw className="mr-2 h-4 w-4" />
            Refresh
          </Button>
          <Button variant="outline" data-test-id="filter-button">
            <Filter className="mr-2 h-4 w-4" />
            Filter
          </Button>
          <Button onClick={() => setIsDialogOpen(true)} data-test-id="add-connector-button">
            <Plus className="mr-2 h-4 w-4" />
            Add Connector
          </Button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6" data-test-id="stats-cards">
        <Card>
          <CardContent className="p-4">
            <div className="text-2xl font-bold">{stats?.total || 0}</div>
            <p className="text-sm text-gray-600 dark:text-gray-400">Total Connectors</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="text-2xl font-bold text-green-600">
              {stats?.active || 0}
            </div>
            <p className="text-sm text-gray-600 dark:text-gray-400">Active</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="text-2xl font-bold text-yellow-600">
              {stats?.inactive || 0}
            </div>
            <p className="text-sm text-gray-600 dark:text-gray-400">Inactive</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="text-2xl font-bold text-red-600">
              {stats?.error || 0}
            </div>
            <p className="text-sm text-gray-600 dark:text-gray-400">Error</p>
          </CardContent>
        </Card>
      </div>

      {/* Connectors Grid */}
      {isLoading ? (
        <div className="flex justify-center items-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
        </div>
      ) : filteredConnectors.length === 0 ? (
        <Card>
          <CardContent className="text-center py-12">
            <p className="text-gray-500 dark:text-gray-400">
              {searchQuery ? 'No connectors found matching your search.' : 'No connectors configured yet.'}
            </p>
            <Button className="mt-4" onClick={() => setIsDialogOpen(true)} data-test-id="add-first-connector-button">
              <Plus className="mr-2 h-4 w-4" />
              Add Your First Connector
            </Button>
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6" data-test-id="connectors-grid">
          {filteredConnectors.map((connector) => (
            <ConnectorCard
              key={connector.id}
              connector={connector}
              onEdit={() => {
                setSelectedConnector(connector)
                setIsDialogOpen(true)
              }}
              onDelete={() => handleDelete(connector.id)}
              onTest={async () => {
                toast('Testing connector...', { icon: '🔄' })
                try {
                  const response = await fetch(`/api/connectors/${connector.id}/test`, {
                    method: 'POST',
                  })
                  const result = await response.json()
                  
                  if (result.success) {
                    toast.success(`✅ Connection successful! (${result.latency}ms)`)
                  } else {
                    toast.error(`❌ Connection failed: ${result.message}`)
                  }
                } catch (error) {
                  toast.error('Failed to test connector')
                }
              }}
            />
          ))}
        </div>
      )}

        {/* Add/Edit Dialog */}
        <ConnectorDialog
          isOpen={isDialogOpen}
          onClose={() => {
            setIsDialogOpen(false)
            setSelectedConnector(null)
          }}
          connector={selectedConnector}
          onSuccess={() => {
            refetch()
            setIsDialogOpen(false)
            setSelectedConnector(null)
          }}
        />
      </div>
    </div>
  )
}