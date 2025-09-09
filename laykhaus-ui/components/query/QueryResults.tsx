'use client'

import { useState } from 'react'
import { QueryResult } from '@/lib/types/query'
import { Button } from '@/components/ui/button'
import { Download, Copy, ChevronLeft, ChevronRight } from 'lucide-react'
import toast from 'react-hot-toast'

interface QueryResultsProps {
  results: QueryResult
}

export function QueryResults({ results }: QueryResultsProps) {
  const [page, setPage] = useState(0)
  const pageSize = 20
  
  if (results.error) {
    return (
      <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
        <p className="text-red-600 dark:text-red-400 font-mono text-sm">{results.error}</p>
      </div>
    )
  }
  
  if (!results.data || results.data.length === 0 || !results.columns) {
    return (
      <div className="text-center py-8 text-muted-foreground">
        No results returned
      </div>
    )
  }
  
  const totalPages = Math.ceil(results.data.length / pageSize)
  const paginatedData = results.data.slice(page * pageSize, (page + 1) * pageSize)
  
  const handleExport = (format: 'csv' | 'json') => {
    if (format === 'csv') {
      const columns = results.columns || Object.keys(results.data[0] || {})
      const csv = [
        columns.join(','),
        ...results.data.map(row => 
          columns.map(col => JSON.stringify(row[col])).join(',')
        )
      ].join('\n')
      
      const blob = new Blob([csv], { type: 'text/csv' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = 'query-results.csv'
      a.click()
    } else {
      const json = JSON.stringify(results.data, null, 2)
      const blob = new Blob([json], { type: 'application/json' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = 'query-results.json'
      a.click()
    }
    
    toast.success(`Exported ${results.data.length} rows as ${format.toUpperCase()}`)
  }
  
  const handleCopy = () => {
    const columns = results.columns || Object.keys(results.data[0] || {})
    const text = results.data.map(row => 
      columns.map(col => row[col]).join('\t')
    ).join('\n')
    
    navigator.clipboard.writeText(text)
    toast.success('Results copied to clipboard')
  }
  
  return (
    <div>
      {/* Actions Bar */}
      <div className="flex justify-between items-center mb-4">
        <div className="text-sm text-muted-foreground">
          Showing {page * pageSize + 1}-{Math.min((page + 1) * pageSize, results.data.length)} of {results.data.length} rows
        </div>
        <div className="flex gap-2">
          <Button size="sm" variant="outline" onClick={handleCopy}>
            <Copy className="mr-2 h-4 w-4" />
            Copy
          </Button>
          <Button size="sm" variant="outline" onClick={() => handleExport('csv')}>
            <Download className="mr-2 h-4 w-4" />
            CSV
          </Button>
          <Button size="sm" variant="outline" onClick={() => handleExport('json')}>
            <Download className="mr-2 h-4 w-4" />
            JSON
          </Button>
        </div>
      </div>
      
      {/* Results Table */}
      <div className="border rounded-md overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-muted">
              <tr>
                {(results.columns || Object.keys(results.data[0] || {})).map((column) => (
                  <th
                    key={column}
                    className="px-4 py-2 text-left text-sm font-medium text-muted-foreground"
                  >
                    {column}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {paginatedData.map((row, rowIndex) => (
                <tr
                  key={rowIndex}
                  className="border-t hover:bg-muted/50 transition-colors"
                >
                  {(results.columns || Object.keys(row)).map((column) => (
                    <td
                      key={column}
                      className="px-4 py-2 text-sm font-mono"
                    >
                      {row[column] === null ? (
                        <span className="text-muted-foreground italic">NULL</span>
                      ) : typeof row[column] === 'object' ? (
                        <span className="text-xs">{JSON.stringify(row[column])}</span>
                      ) : (
                        String(row[column])
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      
      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex justify-center items-center gap-2 mt-4">
          <Button
            size="sm"
            variant="outline"
            onClick={() => setPage(p => Math.max(0, p - 1))}
            disabled={page === 0}
          >
            <ChevronLeft className="h-4 w-4" />
          </Button>
          <span className="text-sm text-muted-foreground">
            Page {page + 1} of {totalPages}
          </span>
          <Button
            size="sm"
            variant="outline"
            onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
            disabled={page === totalPages - 1}
          >
            <ChevronRight className="h-4 w-4" />
          </Button>
        </div>
      )}
    </div>
  )
}