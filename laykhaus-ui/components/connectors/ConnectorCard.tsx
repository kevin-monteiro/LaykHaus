import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { 
  Database, 
  Activity, 
  Globe, 
  Server, 
  Cloud,
  MoreVertical,
  CheckCircle,
  XCircle,
  AlertCircle,
  Edit,
  Trash2,
  Play,
  Pause
} from 'lucide-react'
import { ConnectorConfig } from '@/lib/types/connector'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'

interface ConnectorCardProps {
  connector: ConnectorConfig
  onEdit: () => void
  onDelete?: () => void
  onTest?: () => void
  onToggle?: () => void
}

export function ConnectorCard({ connector, onEdit, onDelete, onTest, onToggle }: ConnectorCardProps) {
  const getIcon = () => {
    switch (connector.type) {
      case 'postgresql':
        return <Database className="h-5 w-5" />
      case 'kafka':
        return <Activity className="h-5 w-5" />
      case 'rest_api':
        return <Globe className="h-5 w-5" />
      case 'spark':
        return <Server className="h-5 w-5" />
      case 'minio':
        return <Cloud className="h-5 w-5" />
      default:
        return <Database className="h-5 w-5" />
    }
  }

  const getStatusIcon = () => {
    switch (connector.status) {
      case 'active':
        return <CheckCircle className="h-4 w-4 text-green-600" />
      case 'inactive':
        return <AlertCircle className="h-4 w-4 text-yellow-600" />
      case 'error':
        return <XCircle className="h-4 w-4 text-red-600" />
    }
  }

  const getStatusColor = () => {
    switch (connector.status) {
      case 'active':
        return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300'
      case 'inactive':
        return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300'
      case 'error':
        return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300'
    }
  }

  return (
    <Card className="hover:shadow-lg transition-shadow">
      <CardHeader className="pb-4">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-primary/10 rounded-lg">
              {getIcon()}
            </div>
            <div>
              <CardTitle className="text-lg">{connector.name}</CardTitle>
              <p className="text-sm text-muted-foreground capitalize">{connector.type}</p>
            </div>
          </div>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="icon">
                <MoreVertical className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onClick={onEdit}>
                <Edit className="mr-2 h-4 w-4" />
                Edit Configuration
              </DropdownMenuItem>
              <DropdownMenuItem onClick={onTest}>
                <Play className="mr-2 h-4 w-4" />
                Test Connection
              </DropdownMenuItem>
              <DropdownMenuItem onClick={onToggle}>
                {connector.status === 'active' ? (
                  <>
                    <Pause className="mr-2 h-4 w-4" />
                    Deactivate
                  </>
                ) : (
                  <>
                    <Play className="mr-2 h-4 w-4" />
                    Activate
                  </>
                )}
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={onDelete} className="text-red-600">
                <Trash2 className="mr-2 h-4 w-4" />
                Delete
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <span className="text-sm text-muted-foreground">Status</span>
            <div className="flex items-center gap-2">
              {getStatusIcon()}
              <Badge variant="secondary" className={getStatusColor()}>
                {connector.status}
              </Badge>
            </div>
          </div>
          
          {connector.config.connection.host && (
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Host</span>
              <span className="text-sm font-mono">{connector.config.connection.host}</span>
            </div>
          )}
          
          {connector.config.connection.database && (
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Database</span>
              <span className="text-sm font-mono">{connector.config.connection.database}</span>
            </div>
          )}
          
          <div className="flex items-center justify-between">
            <span className="text-sm text-muted-foreground">Last Updated</span>
            <span className="text-sm">
              {new Date(connector.metadata.updatedAt).toLocaleDateString()}
            </span>
          </div>
          
          {connector.testResults && connector.testResults.length > 0 && (
            <div className="pt-2 border-t">
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">Last Test</span>
                <span className={`text-sm ${connector.testResults[0].success ? 'text-green-600' : 'text-red-600'}`}>
                  {connector.testResults[0].success ? 'Passed' : 'Failed'}
                </span>
              </div>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}