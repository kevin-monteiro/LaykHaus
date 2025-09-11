'use client'

import { useState, useCallback } from 'react'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { CheckCircle, XCircle, AlertCircle, X } from 'lucide-react'
import { cn } from '@/lib/utils'

export interface NotificationData {
  id: string
  type: 'success' | 'error' | 'warning'
  title: string
  message: string
  timestamp: Date
}

interface InlineNotificationProps {
  notification: NotificationData | null
  onDismiss: () => void
  className?: string
}

export function InlineNotification({ notification, onDismiss, className }: InlineNotificationProps) {
  const [showDetailModal, setShowDetailModal] = useState(false)

  if (!notification) return null

  const getIcon = () => {
    switch (notification.type) {
      case 'success':
        return <CheckCircle className="h-4 w-4 text-green-600" />
      case 'error':
        return <XCircle className="h-4 w-4 text-red-600" />
      case 'warning':
        return <AlertCircle className="h-4 w-4 text-yellow-600" />
    }
  }

  const getAlertVariant = () => {
    return notification.type === 'error' ? 'destructive' : 'default'
  }

  const getBorderColor = () => {
    switch (notification.type) {
      case 'success':
        return 'border-green-200'
      case 'error':
        return 'border-red-200'
      case 'warning':
        return 'border-yellow-200'
      default:
        return 'border-gray-200'
    }
  }

  // Split message into lines and check if it exceeds 2 lines
  const messageLines = notification.message.split('\n')
  const displayLines = messageLines.slice(0, 2)
  const hasMoreContent = messageLines.length > 2 || 
    displayLines.some(line => line.length > 100) // Rough estimate for line length

  return (
    <>
      <Alert 
        variant={getAlertVariant()} 
        className={cn(
          'mb-4 relative pr-12',
          getBorderColor(),
          className
        )}
      >
        <div className="flex items-start gap-3">
          {getIcon()}
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-1">
              <h4 className="font-medium text-sm">{notification.title}</h4>
              <span className="text-xs text-muted-foreground">
                {notification.timestamp.toLocaleTimeString()}
              </span>
            </div>
            <AlertDescription className="text-sm">
              {displayLines.map((line, index) => (
                <div key={index} className={index > 0 ? 'mt-1' : ''}>
                  {line}
                </div>
              ))}
              {hasMoreContent && (
                <Button
                  variant="link"
                  size="sm"
                  className="h-auto p-0 mt-1 text-blue-600 hover:text-blue-800"
                  onClick={() => setShowDetailModal(true)}
                >
                  more
                </Button>
              )}
            </AlertDescription>
          </div>
        </div>
        <Button
          variant="ghost"
          size="sm"
          className="absolute top-2 right-2 h-6 w-6 p-0"
          onClick={onDismiss}
        >
          <X className="h-3 w-3" />
        </Button>
      </Alert>

      {/* Error Detail Modal */}
      <Dialog open={showDetailModal} onOpenChange={setShowDetailModal}>
        <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {getIcon()}
              {notification.title}
            </DialogTitle>
            <DialogDescription>
              Full error details - {notification.timestamp.toLocaleString()}
            </DialogDescription>
          </DialogHeader>
          <div className="mt-4">
            <div className="bg-muted/50 p-4 rounded-md">
              <pre className="whitespace-pre-wrap text-sm font-mono">
                {notification.message}
              </pre>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </>
  )
}

// Hook for managing inline notifications
export function useInlineNotifications() {
  const [notifications, setNotifications] = useState<NotificationData[]>([])

  const addNotification = useCallback((
    type: 'success' | 'error' | 'warning',
    title: string,
    message: string
  ) => {
    const notification: NotificationData = {
      id: Date.now().toString(),
      type,
      title,
      message,
      timestamp: new Date(),
    }
    setNotifications(prev => [notification, ...prev.slice(0, 2)]) // Keep max 3 notifications
  }, [])

  const removeNotification = useCallback((id: string) => {
    setNotifications(prev => prev.filter(n => n.id !== id))
  }, [])

  const clearAll = useCallback(() => {
    setNotifications([])
  }, [])

  const addSuccess = useCallback((title: string, message: string = '') => 
    addNotification('success', title, message), [addNotification])

  const addError = useCallback((title: string, message: string = '') => 
    addNotification('error', title, message), [addNotification])

  const addWarning = useCallback((title: string, message: string = '') => 
    addNotification('warning', title, message), [addNotification])

  return {
    notifications,
    addNotification,
    addSuccess,
    addError,
    addWarning,
    removeNotification,
    clearAll,
  }
}