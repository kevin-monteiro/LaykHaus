'use client'

import { useState, useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import * as z from 'zod'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ConnectorConfig } from '@/lib/types/connector'
import { Play, Save, Eye, EyeOff } from 'lucide-react'
import { useCreateConnector, useUpdateConnector, useTestConnector } from '@/lib/hooks/useConnectors'

const connectorSchema = z.object({
  name: z.string().min(1, 'Name is required'),
  type: z.enum(['postgresql', 'kafka', 'rest_api', 'minio']),
  // PostgreSQL fields
  host: z.string().optional(),
  port: z.number().optional(),
  database: z.string().optional(),
  username: z.string().optional(),
  password: z.string().optional(),
  schema: z.string().optional(),
  // Kafka fields
  brokers: z.string().optional(),
  topics: z.string().optional(),
  groupId: z.string().optional(),
  // REST API fields
  baseUrl: z.string().optional(),
  authType: z.string().optional(),
  apiKey: z.string().optional(),
})

type ConnectorFormData = z.infer<typeof connectorSchema>

interface ConnectorDialogProps {
  isOpen: boolean
  onClose: () => void
  connector?: ConnectorConfig | null
  onSuccess?: () => void
}

export function ConnectorDialog({ isOpen, onClose, connector, onSuccess }: ConnectorDialogProps) {
  const [isTesting, setIsTesting] = useState(false)
  const [showPassword, setShowPassword] = useState(false)
  const [showApiKey, setShowApiKey] = useState(false)
  const createConnector = useCreateConnector()
  const updateConnector = useUpdateConnector()
  const testConnector = useTestConnector()

  const form = useForm<ConnectorFormData>({
    resolver: zodResolver(connectorSchema),
    defaultValues: {
      name: '',
      type: 'postgresql',
    },
  })

  useEffect(() => {
    if (connector) {
      // Parse existing connector data
      const formData: any = {
        name: connector.name,
        type: connector.type === 'rest' ? 'rest_api' : connector.type,
      }
      
      // Map config based on type
      if (connector.type === 'postgresql') {
        formData.host = connector.config.connection?.host
        formData.port = connector.config.connection?.port
        formData.database = connector.config.connection?.database
        formData.username = (connector.config.authentication as any)?.username
        formData.password = (connector.config.authentication as any)?.password
        formData.schema = (connector.config.schema as any)?.name || (connector.config.schema as any)?.default
      } else if (connector.type === 'kafka') {
        formData.brokers = connector.config.connection?.brokers
        formData.topics = Array.isArray(connector.config.connection?.topics) 
          ? connector.config.connection.topics.join(', ') 
          : connector.config.connection?.topics
        formData.groupId = connector.config.connection?.group_id
      } else if (connector.type === 'rest' || connector.type === 'rest_api') {
        formData.baseUrl = connector.config.connection?.base_url
        formData.authType = connector.config.authentication?.type || 'none'
        formData.apiKey = (connector.config.authentication as any)?.api_key
      }
      
      form.reset(formData)
    } else {
      form.reset({
        name: '',
        type: 'postgresql',
      })
    }
  }, [connector, form])

  const onSubmit = async (data: ConnectorFormData) => {
    try {
      let connectorData: any = {
        name: data.name,
        type: data.type === 'rest_api' ? 'rest' : data.type,
        config: {}
      }

      // Build config based on connector type
      if (data.type === 'postgresql') {
        connectorData.config = {
          host: data.host,
          port: data.port,
          database: data.database,
          username: data.username,
          password: data.password,
          schema: data.schema || 'public',
        }
      } else if (data.type === 'kafka') {
        // Convert comma-separated topics to array
        const topicsArray = data.topics ? data.topics.split(',').map(t => t.trim()) : []
        connectorData.config = {
          brokers: data.brokers,
          topics: topicsArray,
          group_id: data.groupId || 'laykhaus-consumer',
        }
      } else if (data.type === 'rest_api') {
        connectorData.config = {
          base_url: data.baseUrl,
          auth_type: data.authType || 'none',
        }
        // Add auth config if needed
        if (data.authType === 'api_key' && data.apiKey) {
          connectorData.config.auth_config = {
            api_key: data.apiKey,
          }
        }
      }

      if (connector) {
        await updateConnector.mutateAsync({ id: connector.id, data: connectorData })
      } else {
        await createConnector.mutateAsync(connectorData)
      }
      onSuccess?.()
    } catch (error) {
      // Error is handled by the mutation hooks
    }
  }

  const testConnection = async () => {
    if (!connector) return
    setIsTesting(true)
    try {
      await testConnector.mutateAsync(connector.id)
    } finally {
      setIsTesting(false)
    }
  }

  const connectorType = form.watch('type')

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>{connector ? 'Edit Connector' : 'Add New Connector'}</DialogTitle>
          <DialogDescription>
            Configure your data source connection details
          </DialogDescription>
        </DialogHeader>

        <Form {...form}>
          <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-6">
            <Tabs defaultValue="general" className="w-full">
              <TabsList className="grid w-full grid-cols-3">
                <TabsTrigger value="general">General</TabsTrigger>
                <TabsTrigger value="connection">Connection</TabsTrigger>
                <TabsTrigger value="advanced">Advanced</TabsTrigger>
              </TabsList>

              <TabsContent value="general" className="space-y-4 mt-4">
                <FormField
                  control={form.control}
                  name="name"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Connector Name</FormLabel>
                      <FormControl>
                        <Input placeholder="My Database" {...field} />
                      </FormControl>
                      <FormDescription>
                        A friendly name to identify this connector
                      </FormDescription>
                      <FormMessage />
                    </FormItem>
                  )}
                />

                <FormField
                  control={form.control}
                  name="type"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Connector Type</FormLabel>
                      <Select onValueChange={field.onChange} defaultValue={field.value}>
                        <FormControl>
                          <SelectTrigger>
                            <SelectValue placeholder="Select a connector type" />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          <SelectItem value="postgresql">PostgreSQL</SelectItem>
                          <SelectItem value="kafka">Apache Kafka</SelectItem>
                          <SelectItem value="rest_api">REST API</SelectItem>
                          <SelectItem value="minio">MinIO</SelectItem>
                        </SelectContent>
                      </Select>
                      <FormDescription>
                        The type of data source you want to connect
                      </FormDescription>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              </TabsContent>

              <TabsContent value="connection" className="space-y-4 mt-4">
                {connectorType === 'postgresql' && (
                  <>
                    <FormField
                      control={form.control}
                      name="host"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Host</FormLabel>
                          <FormControl>
                            <Input placeholder="localhost" {...field} />
                          </FormControl>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                    <FormField
                      control={form.control}
                      name="port"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Port</FormLabel>
                          <FormControl>
                            <Input 
                              type="number" 
                              placeholder="5432"
                              {...field}
                              onChange={(e) => field.onChange(parseInt(e.target.value))}
                            />
                          </FormControl>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                    <FormField
                      control={form.control}
                      name="database"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Database</FormLabel>
                          <FormControl>
                            <Input placeholder="mydb" {...field} />
                          </FormControl>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                    <FormField
                      control={form.control}
                      name="username"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Username</FormLabel>
                          <FormControl>
                            <Input placeholder="user" {...field} />
                          </FormControl>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                    <FormField
                      control={form.control}
                      name="password"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Password</FormLabel>
                          <FormControl>
                            <div className="relative">
                              <Input 
                                type={showPassword ? "text" : "password"} 
                                placeholder="••••••••" 
                                {...field} 
                              />
                              <Button
                                type="button"
                                variant="ghost"
                                size="sm"
                                className="absolute right-0 top-0 h-full px-3 py-2 hover:bg-transparent"
                                onClick={() => setShowPassword(!showPassword)}
                              >
                                {showPassword ? (
                                  <EyeOff className="h-4 w-4" />
                                ) : (
                                  <Eye className="h-4 w-4" />
                                )}
                              </Button>
                            </div>
                          </FormControl>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                    <FormField
                      control={form.control}
                      name="schema"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Schema</FormLabel>
                          <FormControl>
                            <Input placeholder="public" {...field} />
                          </FormControl>
                          <FormDescription>
                            Database schema (default: public)
                          </FormDescription>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                  </>
                )}

                {connectorType === 'kafka' && (
                  <>
                    <FormField
                      control={form.control}
                      name="brokers"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Brokers</FormLabel>
                          <FormControl>
                            <Input placeholder="localhost:9092" {...field} />
                          </FormControl>
                          <FormDescription>
                            Comma-separated list of Kafka brokers
                          </FormDescription>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                    <FormField
                      control={form.control}
                      name="topics"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Topics</FormLabel>
                          <FormControl>
                            <Input placeholder="topic1,topic2,topic3" {...field} />
                          </FormControl>
                          <FormDescription>
                            Comma-separated list of Kafka topics
                          </FormDescription>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                    <FormField
                      control={form.control}
                      name="groupId"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Consumer Group ID</FormLabel>
                          <FormControl>
                            <Input placeholder="laykhaus-consumer" {...field} />
                          </FormControl>
                          <FormDescription>
                            Kafka consumer group identifier
                          </FormDescription>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                  </>
                )}

                {connectorType === 'rest_api' && (
                  <>
                    <FormField
                      control={form.control}
                      name="baseUrl"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Base URL</FormLabel>
                          <FormControl>
                            <Input placeholder="http://localhost:8080" {...field} />
                          </FormControl>
                          <FormDescription>
                            The base URL of the REST API endpoint
                          </FormDescription>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                    <FormField
                      control={form.control}
                      name="authType"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Authentication Type</FormLabel>
                          <Select onValueChange={field.onChange} defaultValue={field.value || 'none'}>
                            <FormControl>
                              <SelectTrigger>
                                <SelectValue placeholder="Select auth type" />
                              </SelectTrigger>
                            </FormControl>
                            <SelectContent>
                              <SelectItem value="none">None</SelectItem>
                              <SelectItem value="api_key">API Key</SelectItem>
                              <SelectItem value="bearer">Bearer Token</SelectItem>
                              <SelectItem value="basic">Basic Auth</SelectItem>
                            </SelectContent>
                          </Select>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                    {form.watch('authType') === 'api_key' && (
                      <FormField
                        control={form.control}
                        name="apiKey"
                        render={({ field }) => (
                          <FormItem>
                            <FormLabel>API Key</FormLabel>
                            <FormControl>
                              <div className="relative">
                                <Input 
                                  type={showApiKey ? "text" : "password"} 
                                  placeholder="••••••••" 
                                  {...field} 
                                />
                                <Button
                                  type="button"
                                  variant="ghost"
                                  size="sm"
                                  className="absolute right-0 top-0 h-full px-3 py-2 hover:bg-transparent"
                                  onClick={() => setShowApiKey(!showApiKey)}
                                >
                                  {showApiKey ? (
                                    <EyeOff className="h-4 w-4" />
                                  ) : (
                                    <Eye className="h-4 w-4" />
                                  )}
                                </Button>
                              </div>
                            </FormControl>
                            <FormMessage />
                          </FormItem>
                        )}
                      />
                    )}
                  </>
                )}
              </TabsContent>

              <TabsContent value="advanced" className="space-y-4 mt-4">
                <p className="text-sm text-muted-foreground">
                  Advanced configuration options will be available here
                </p>
              </TabsContent>
            </Tabs>

            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={testConnection}
                disabled={isTesting || createConnector.isPending || updateConnector.isPending || !connector}
              >
                <Play className="mr-2 h-4 w-4" />
                {isTesting ? 'Testing...' : 'Test Connection'}
              </Button>
              <Button type="submit" disabled={createConnector.isPending || updateConnector.isPending || isTesting}>
                <Save className="mr-2 h-4 w-4" />
                {createConnector.isPending || updateConnector.isPending ? 'Saving...' : connector ? 'Update' : 'Create'}
              </Button>
            </DialogFooter>
          </form>
        </Form>
      </DialogContent>
    </Dialog>
  )
}