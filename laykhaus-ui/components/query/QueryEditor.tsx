'use client'

import { useRef } from 'react'
import { Button } from '@/components/ui/button'
import { Play } from 'lucide-react'

interface QueryEditorProps {
  value: string
  onChange: (value: string) => void
  onExecute?: () => void
  placeholder?: string
  height?: string
}

export function QueryEditor({
  value,
  onChange,
  onExecute,
  placeholder = 'SELECT * FROM ...',
  height = '400px',
}: QueryEditorProps) {
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  const handleKeyDown = (e: React.KeyboardEvent) => {
    // Execute on Ctrl/Cmd + Enter
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault()
      onExecute?.()
    }
    
    // Tab handling for indentation
    if (e.key === 'Tab') {
      e.preventDefault()
      const start = textareaRef.current?.selectionStart || 0
      const end = textareaRef.current?.selectionEnd || 0
      const newValue = value.substring(0, start) + '  ' + value.substring(end)
      onChange(newValue)
      
      // Set cursor position after the tab
      setTimeout(() => {
        if (textareaRef.current) {
          textareaRef.current.selectionStart = start + 2
          textareaRef.current.selectionEnd = start + 2
        }
      }, 0)
    }
  }

  return (
    <div className="relative">
      <div className="absolute top-2 right-2 z-10 flex gap-2">
        <Button
          size="sm"
          variant="ghost"
          onClick={onExecute}
          title="Execute Query (Ctrl/Cmd + Enter)"
        >
          <Play className="h-4 w-4" />
        </Button>
      </div>
      
      <textarea
        ref={textareaRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        className="w-full p-4 pr-20 font-mono text-sm bg-background border rounded-md resize-none focus:outline-none focus:ring-2 focus:ring-ring"
        style={{ height }}
        spellCheck={false}
      />
      
      <div className="mt-2 text-xs text-muted-foreground">
        Press <kbd className="px-1 py-0.5 bg-muted rounded">Ctrl</kbd> + <kbd className="px-1 py-0.5 bg-muted rounded">Enter</kbd> to execute
      </div>
    </div>
  )
}