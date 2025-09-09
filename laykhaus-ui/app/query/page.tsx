'use client'

import { useState } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Header } from '@/components/layout/Header'
import { QueryEditor } from '@/components/query/QueryEditor'
import { QueryResults } from '@/components/query/QueryResults'
import { SchemaExplorer } from '@/components/query/SchemaExplorer'
import { QueryHistory } from '@/components/query/QueryHistory'
import { Play, Save, History, Database, Code2 } from 'lucide-react'
import { useExecuteQuery, useSaveQuery } from '@/lib/hooks/useQuery'

export default function QueryPage() {
  const [query, setQuery] = useState('')
  const [queryName] = useState('')
  const [selectedDataSources] = useState<string[]>([])
  const [activeTab, setActiveTab] = useState('editor')
  
  const executeQuery = useExecuteQuery()
  const saveQuery = useSaveQuery()

  const handleExecute = () => {
    if (!query.trim()) {
      return
    }
    
    executeQuery.mutate({
      query,
      dataSources: selectedDataSources,
    })
  }

  const handleSave = () => {
    if (!query.trim() || !queryName.trim()) {
      return
    }
    
    saveQuery.mutate({
      name: queryName,
      query,
      type: 'federated',
      dataSources: selectedDataSources,
    })
  }

  return (
    <div className="min-h-screen bg-gradient-to-b from-gray-50 to-white dark:from-gray-900 dark:to-gray-800" data-test-id="query-page">
      <Header />
      <div className="container mx-auto px-4 py-8">
        {/* Page Description */}
        <div className="mb-8 text-center" data-test-id="page-description">
          <p className="text-gray-600 dark:text-gray-400" data-test-id="page-tagline">
            Build and execute federated queries across your data sources
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Left Sidebar - Schema Explorer */}
        <div className="lg:col-span-1" data-test-id="schema-explorer-container">
          <Card className="h-full" data-test-id="schema-explorer-card">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Database className="h-4 w-4" />
                Schema Explorer
              </CardTitle>
              <CardDescription>Browse available tables and fields</CardDescription>
            </CardHeader>
            <CardContent>
              <SchemaExplorer 
                onTableSelect={(table) => {
                  setQuery(query + (query ? '\n' : '') + `SELECT * FROM ${table}`)
                }}
              />
            </CardContent>
          </Card>
        </div>

        {/* Main Content Area */}
        <div className="lg:col-span-3 space-y-6" data-test-id="main-content-area">
          {/* Query Editor Tabs */}
          <Card data-test-id="query-editor-card">
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle>Query Editor</CardTitle>
                  <CardDescription>Write and execute SQL queries across your data sources</CardDescription>
                </div>
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setActiveTab('history')}
                    data-test-id="history-button"
                  >
                    <History className="mr-2 h-4 w-4" />
                    History
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleSave}
                    disabled={!query.trim() || saveQuery.isPending}
                    data-test-id="save-query-button"
                  >
                    <Save className="mr-2 h-4 w-4" />
                    Save
                  </Button>
                  <Button
                    size="sm"
                    onClick={handleExecute}
                    disabled={!query.trim() || executeQuery.isPending}
                    data-test-id="execute-query-button"
                  >
                    <Play className="mr-2 h-4 w-4" />
                    {executeQuery.isPending ? 'Executing...' : 'Execute'}
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <Tabs value={activeTab} onValueChange={setActiveTab} data-test-id="query-tabs">
                <TabsList className="grid w-full grid-cols-2">
                  <TabsTrigger value="editor">
                    <Code2 className="mr-2 h-4 w-4" />
                    SQL Editor
                  </TabsTrigger>
                  <TabsTrigger value="history">
                    <History className="mr-2 h-4 w-4" />
                    History
                  </TabsTrigger>
                </TabsList>
                
                <TabsContent value="editor" className="mt-4">
                  <QueryEditor
                    value={query}
                    onChange={setQuery}
                    onExecute={handleExecute}
                  />
                </TabsContent>
                
                <TabsContent value="history" className="mt-4">
                  <QueryHistory
                    onSelectQuery={(historicalQuery) => {
                      setQuery(historicalQuery.query)
                      setActiveTab('editor')
                    }}
                  />
                </TabsContent>
              </Tabs>
            </CardContent>
          </Card>

          {/* Query Results */}
          {executeQuery.data && (
            <Card data-test-id="query-results-card">
              <CardHeader>
                <CardTitle>Query Results</CardTitle>
                <CardDescription>
                  {executeQuery.data.rowCount} rows returned in {executeQuery.data.executionTime}ms
                </CardDescription>
              </CardHeader>
              <CardContent>
                <QueryResults results={executeQuery.data} />
              </CardContent>
            </Card>
          )}
        </div>
        </div>
      </div>
    </div>
  )
}