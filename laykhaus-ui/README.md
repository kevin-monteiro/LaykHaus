# LaykHaus UI - Configuration Manager

Zero-code configuration interface for LaykHaus federated data lakehouse platform.

## 🚀 Features

- **Connector Management** - Add, configure, and test data source connections
- **Query Builder** - Visual and SQL-based federated query builder
- **Streaming Interface** - Manage Kafka topics and stream processors
- **Analytics Workbench** - Submit and monitor Spark jobs
- **Real-time Monitoring** - Live dashboards and metrics
- **95% Test Coverage** - Comprehensive testing with Vitest

## 📋 Prerequisites

- Node.js 18+
- pnpm 8+
- LaykHaus Core running on http://localhost:8000

## 🛠️ Installation

```bash
# Install dependencies
pnpm install

# Start development server
pnpm dev

# Build for production
pnpm build

# Start production server
pnpm start
```

## 🧪 Testing

```bash
# Run unit tests
pnpm test

# Run tests with UI
pnpm test:ui

# Run tests in watch mode
pnpm test:watch

# Generate coverage report (95% target)
pnpm coverage

# Run E2E tests
pnpm test:e2e
```

## 🏗️ Project Structure

```
laykhaus-ui/
├── app/                    # Next.js 14 App Router pages
│   ├── connectors/        # Connector management
│   ├── query/            # Query builder
│   ├── streaming/        # Kafka streaming
│   └── analytics/        # Spark analytics
├── components/            # React components
│   ├── ui/               # shadcn/ui components
│   ├── connectors/       # Connector components
│   ├── query/           # Query components
│   └── __tests__/       # Component tests
├── lib/                  # Utilities and services
│   ├── api/             # API client
│   ├── hooks/           # Custom React hooks
│   ├── stores/          # Zustand stores
│   └── types/           # TypeScript types
└── tests/               # Test configuration
```

## 🔧 Configuration

### Environment Variables

Create a `.env.local` file:

```env
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_GRAPHQL_URL=http://localhost:8000/graphql
NEXT_PUBLIC_WS_URL=ws://localhost:8000/ws
```

### API Integration

The UI connects to LaykHaus Core via:
- REST API for CRUD operations
- GraphQL for complex queries
- WebSocket for real-time updates

## 📊 Test Coverage Requirements

The project maintains **95% test coverage** across:
- Statements: 95%
- Branches: 95%
- Functions: 95%
- Lines: 95%

Coverage reports are generated in `/coverage` directory.

## 🎨 UI Components

Built with **shadcn/ui** (Radix UI + Tailwind CSS):
- Fully accessible (WCAG 2.1 AA)
- Dark/light mode support
- Responsive design
- Keyboard navigation

## 🔌 Key Features

### Connector Management
- Support for PostgreSQL, Kafka, REST API, Spark, MinIO
- Connection testing and validation
- Schema discovery and browsing
- Credential management

### Query Builder
- SQL editor with syntax highlighting
- Visual query builder (drag-and-drop)
- Federated query execution
- Query history and templates
- Results export (CSV/JSON)

### Streaming Interface
- Kafka topic management
- Consumer group monitoring
- Stream processor configuration
- Real-time metrics

### Analytics Workbench
- Spark job submission
- Resource allocation
- Job monitoring and logs
- Results visualization

## 🚢 Deployment

### Docker

```dockerfile
FROM node:18-alpine AS builder
WORKDIR /app
COPY package.json pnpm-lock.yaml ./
RUN npm install -g pnpm && pnpm install --frozen-lockfile
COPY . .
RUN pnpm build

FROM node:18-alpine
WORKDIR /app
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public
COPY --from=builder /app/package.json ./
RUN npm install -g pnpm && pnpm install --production
EXPOSE 3000
CMD ["pnpm", "start"]
```

### Build and Run

```bash
docker build -t laykhaus-ui .
docker run -p 3000:3000 laykhaus-ui
```

## 📝 Configuration Output

The UI generates YAML/JSON configuration files that LaykHaus reads:

```yaml
connectors:
  - id: "conn_001"
    name: "Production DB"
    type: "postgresql"
    config:
      host: "${DB_HOST}"
      port: 5432
      database: "production"

federation_rules:
  - virtual_table: "customer_360"
    sources:
      - connector_id: "conn_001"
        table: "customers"
```

## 🔐 Security

- OAuth2/OIDC authentication
- Role-based access control (RBAC)
- Encrypted credential storage
- SQL injection prevention
- XSS protection
- CSRF tokens

## 📈 Performance

- Initial load: <2 seconds
- API response: <500ms
- Real-time updates: <100ms latency
- Bundle size: <500KB
- Lighthouse score: >90

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests (maintain 95% coverage)
4. Submit a pull request

## 📄 License

MIT License - See LICENSE file for details