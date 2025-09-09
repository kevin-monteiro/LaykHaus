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
  
  // Mock data structure for demonstration
  const mockSchemas = {
    databases: [
      {
        name: 'postgres',
        tables: [
          {
            name: 'customers',
            columns: ['id', 'name', 'email', 'created_at'],
          },
          {
            name: 'orders',
            columns: ['id', 'customer_id', 'total', 'status', 'created_at'],
          },
        ],
      },
      {
        name: 'kafka',
        tables: [
          {
            name: 'events',
            columns: ['event_id', 'event_type', 'payload', 'timestamp'],
          },
        ],
      },
    ],
  }
  
  const data = schemas || mockSchemas
  
  if (isLoading) {
    return (
      <div className="flex justify-center py-4">
        <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-primary"></div>
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
                          onTableSelect?.(`${db.name}.${table.name}`)
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
                              onClick={() => onFieldSelect?.(`${db.name}.${table.name}.${column}`)}
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