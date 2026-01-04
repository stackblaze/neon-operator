# Neon Compute Proxy

The Neon Compute Proxy is a PostgreSQL connection proxy that automatically scales compute instances from zero when connections are requested.

## Features

- **Auto-scaling**: Automatically wakes up scaled-down compute instances (0 replicas) when connections are requested
- **Connection forwarding**: Proxies PostgreSQL connections between clients and compute instances
- **Kubernetes integration**: Uses Kubernetes API to check deployment status and scale instances
- **Startup message parsing**: Extracts compute ID from PostgreSQL startup messages

## How It Works

1. **Connection Interception**: The proxy listens for incoming PostgreSQL connections
2. **Startup Message Parsing**: Reads the PostgreSQL startup message to extract the compute ID (from the `database` parameter)
3. **Scale-up Check**: Checks if the compute deployment is scaled down (0 replicas)
4. **Wake-up**: If scaled down, calls the control plane `/wake` endpoint to scale up the deployment
5. **Readiness Wait**: Waits for the deployment to have ready pods
6. **Connection Forwarding**: Forwards the connection to the compute instance

## Configuration

The proxy can be configured via environment variables:

- `PROXY_LISTEN_ADDR`: Address to listen on (default: `0.0.0.0:5432`)
- `CONTROL_PLANE_URL`: URL of the control plane service (default: `http://neon-controlplane.neon:8081`)

## Usage

### Running Locally

```bash
# Build the proxy
make build-proxy

# Run the proxy
./bin/proxy
```

### Running in Kubernetes

1. Build and push the Docker image:
```bash
make docker-build-proxy
make docker-push-proxy
```

2. Deploy the proxy (you'll need to create deployment manifests):
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: neon-proxy
  namespace: neon
spec:
  replicas: 1
  selector:
    matchLabels:
      app: neon-proxy
  template:
    metadata:
      labels:
        app: neon-proxy
    spec:
      serviceAccountName: neon-operator-manager
      containers:
      - name: proxy
        image: neon-proxy:latest
        env:
        - name: PROXY_LISTEN_ADDR
          value: "0.0.0.0:5432"
        - name: CONTROL_PLANE_URL
          value: "http://neon-controlplane.neon:8081"
        ports:
        - containerPort: 5432
          name: postgres
```

3. Expose the proxy via a Service:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: neon-proxy
  namespace: neon
spec:
  selector:
    app: neon-proxy
  ports:
  - port: 5432
    targetPort: 5432
    name: postgres
  type: LoadBalancer  # or ClusterIP, NodePort, etc.
```

## Connection Flow

1. Client connects to proxy (e.g., `psql -h proxy-host -d branch-name`)
2. Proxy reads startup message and extracts `database` parameter (which is the branch/compute name)
3. Proxy checks if compute deployment exists and is scaled down
4. If scaled down, proxy calls `POST /compute/api/v2/computes/{compute_id}/wake`
5. Proxy waits for deployment to have ready pods (up to 60 seconds)
6. Proxy connects to compute service and forwards the startup message
7. Proxy streams data bidirectionally between client and compute

## Limitations

- Currently extracts compute ID from the `database` parameter in the startup message
- In production, you may want to use SNI (Server Name Indication) for TLS connections
- The proxy assumes service naming pattern: `{branch-name}-postgres`
- Startup message parsing is simplified - full PostgreSQL protocol parsing would be more robust

## Future Improvements

- Support for TLS/SNI-based compute ID extraction
- Connection pooling
- Metrics and observability
- Health checks
- Graceful shutdown
- Support for multiple compute instances per connection

