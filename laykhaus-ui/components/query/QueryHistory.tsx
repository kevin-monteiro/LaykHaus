'use client'

import { useEffect, useState } from 'react'
import { QueryInterface } from '@/lib/types/query'
import { useQueryHistory } from '@/lib/hooks/useQuery'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Clock, CheckCircle, XCircle, Loader2 } from 'lucide-react'
import { formatDistanceToNow } from 'date-fns'

interface QueryHistoryProps {
  onSelectQuery: (query: QueryInterface) => void
}

export function QueryHistory({ onSelectQuery }: QueryHistoryProps) {
  const { data: history, isLoading } = useQueryHistory()
  const [localHistory, setLocalHistory] = useState<QueryInterface[]>([])
  
  // Load history from localStorage on mount and listen for updates
  useEffect(() => {
    const loadHistory = () => {
      const stored = localStorage.getItem('queryHistory')
      if (stored) {
        try {
          const parsed = JSON.parse(stored)
          setLocalHistory(parsed)
        } catch (e) {
          console.error('Failed to parse query history:', e)
        }
      }
    }
    
    loadHistory()
    
    // Listen for storage events to update history
    window.addEventListener('storage', loadHistory)
    
    return () => {
      window.removeEventListener('storage', loadHistory)
    }
  }, [])
  
  // Use API history if available, otherwise use localStorage history
  const queries = history || localHistory
  
  if (isLoading) {
    return (
      <div className="flex justify-center py-8">
        <Loader2 className="h-6 w-6 animate-spin" />
      </div>
    )
  }
  
  if (!queries || queries.length === 0) {
    return (
      <div className="text-center py-8 text-muted-foreground">
        <Clock className="h-12 w-12 mx-auto mb-4 opacity-50" />
        <p>No query history available</p>
      </div>
    )
  }
  
  return (
    <div className="space-y-2">
      {queries.map((query) => {
        const lastExecution = query.executionHistory[0]
        const statusIcon = lastExecution?.status === 'success' 
          ? <CheckCircle className="h-4 w-4 text-green-600" />
          : lastExecution?.status === 'failure'
          ? <XCircle className="h-4 w-4 text-red-600" />
          : <Loader2 className="h-4 w-4 animate-spin" />
        
        return (
          <div
            key={query.id}
            className="p-4 border rounded-lg hover:bg-muted/50 transition-colors cursor-pointer"
            onClick={() => onSelectQuery(query)}
          >
            <div className="flex items-start justify-between">
              <div className="flex-1">
                <div className="flex items-center gap-2 mb-2">
                  {statusIcon}
                  <h4 className="font-medium">{query.name || 'Untitled Query'}</h4>
                  <Badge variant="secondary" className="text-xs">
                    {query.type}
                  </Badge>
                  {query.dataSources.map((ds) => (
                    <Badge key={ds} variant="outline" className="text-xs">
                      {ds}
                    </Badge>
                  ))}
                </div>
                
                <p className="text-sm font-mono text-muted-foreground line-clamp-2">
                  {query.query}
                </p>
                
                {lastExecution && (
                  <div className="flex items-center gap-4 mt-2 text-xs text-muted-foreground">
                    <span>
                      {formatDistanceToNow(new Date(lastExecution.timestamp), { addSuffix: true })}
                    </span>
                    {lastExecution.duration && (
                      <span>{lastExecution.duration}ms</span>
                    )}
                    {lastExecution.error && (
                      <span className="text-red-600">{lastExecution.error}</span>
                    )}
                  </div>
                )}
              </div>
              
              <Button
                size="sm"
                variant="ghost"
                onClick={(e) => {
                  e.stopPropagation()
                  onSelectQuery(query)
                }}
              >
                Use Query
              </Button>
            </div>
          </div>
        )
      })}
    </div>
  )
}