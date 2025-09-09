'use client'

import { useState } from 'react'
import { ChevronRight, ChevronDown, Table, Database, Columns3 } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { useSchemas } from '@/lib/hooks/useQuery'

interface SchemaExplorerProps {
  onTableSelect?: (table: string) => void
  onFieldSelect?: (field: string) => void
}

export function SchemaExplorer({ onTableSelect, onFieldSelect }: SchemaExplorerProps) {
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set())
  const [searchQuery, setSearchQuery] = useState('')
  
  const { data: schemas, isLoading } = useSchemas()
  
  const toggleNode = (nodeId: string) => {
    const newExpanded = new Set(expandedNodes)
    if (newExpanded.has(nodeId)) {
      newExpanded.delete(nodeId)
    } else {
      newExpanded.add(nodeId)
    }
    setExpandedNodes(newExpanded)
  }
  
  // Use only real data from the API
  const data = schemas || { databases: [] }
  
  if (isLoading) {
    return (
      <div className="flex justify-center py-4">
        <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-primary"></div>
      </div>
    )
  }
  
  if (!data.databases || data.databases.length === 0) {
    return (
      <div className="text-center py-4 text-muted-foreground text-sm">
        <Database className="h-8 w-8 mx-auto mb-2 opacity-50" />
        <p>No schemas available</p>
        <p className="text-xs mt-1">Add connectors to see schemas</p>
      </div>
    )
  }
  
  return (
    <div className="space-y-2">
      <Input
        placeholder="Search tables..."
        value={searchQuery}
        onChange={(e) => setSearchQuery(e.target.value)}
        className="mb-4"
      />
      
      <div className="space-y-1">
        {data.databases.map((db: any) => (
          <div key={db.name}>
            <Button
              variant="ghost"
              size="sm"
              className="w-full justify-start"
              onClick={() => toggleNode(`db-${db.name}`)}
            >
              {expandedNodes.has(`db-${db.name}`) ? (
                <ChevronDown className="mr-2 h-4 w-4" />
              ) : (
                <ChevronRight className="mr-2 h-4 w-4" />
              )}
              <Database className="mr-2 h-4 w-4" />
              <span className="font-medium">{db.name}</span>
            </Button>
            
            {expandedNodes.has(`db-${db.name}`) && (
              <div className="ml-4 space-y-1">
                {db.tables
                  .filter((table: any) => 
                    !searchQuery || 
                    table.name.toLowerCase().includes(searchQuery.toLowerCase())
                  )
                  .map((table: any) => (
                    <div key={table.name}>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="w-full justify-start"
                        onClick={() => {
                          toggleNode(`table-${db.name}-${table.name}`)
                          // Add backticks if table name contains special characters
                          const tableName = table.name.includes('-') || table.name.includes(' ') 
                            ? `\`${table.name}\`` 
                            : table.name
                          onTableSelect?.(`${db.name}.${tableName}`)
                        }}
                      >
                        {expandedNodes.has(`table-${db.name}-${table.name}`) ? (
                          <ChevronDown className="mr-2 h-3 w-3" />
                        ) : (
                          <ChevronRight className="mr-2 h-3 w-3" />
                        )}
                        <Table className="mr-2 h-3 w-3" />
                        <span className="text-sm">{table.name}</span>
                      </Button>
                      
                      {expandedNodes.has(`table-${db.name}-${table.name}`) && (
                        <div className="ml-4 space-y-0.5">
                          {table.columns.map((column: any) => (
                            <Button
                              key={column}
                              variant="ghost"
                              size="sm"
                              className="w-full justify-start text-xs"
                              onClick={() => {
                                const tableName = table.name.includes('-') || table.name.includes(' ') 
                                  ? `\`${table.name}\`` 
                                  : table.name
                                onFieldSelect?.(`${db.name}.${tableName}.${column}`)
                              }}
                            >
                              <Columns3 className="mr-2 h-3 w-3" />
                              <span className="font-mono">{column}</span>
                            </Button>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}