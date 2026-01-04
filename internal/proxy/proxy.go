package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Proxy handles PostgreSQL connections and auto-scaling
type Proxy struct {
	k8sClient    client.Client
	controlPlane string
	logger       *slog.Logger
	listenAddr   string
}

// Config holds proxy configuration
type Config struct {
	ListenAddr   string
	ControlPlane string
	K8sClient    client.Client
	Logger       *slog.Logger
}

// New creates a new proxy instance
func New(cfg Config) *Proxy {
	return &Proxy{
		k8sClient:    cfg.K8sClient,
		controlPlane: cfg.ControlPlane,
		logger:       cfg.Logger,
		listenAddr:   cfg.ListenAddr,
	}
}

// Run starts the proxy server
func (p *Proxy) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", p.listenAddr, err)
	}
	defer listener.Close()

	p.logger.Info("Proxy listening", "addr", p.listenAddr)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			conn, err := listener.Accept()
			if err != nil {
				p.logger.Error("Failed to accept connection", "error", err)
				continue
			}

			go p.handleConnection(ctx, conn)
		}
	}
}

// handleConnection processes a single PostgreSQL connection
func (p *Proxy) handleConnection(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close()

	p.logger.Info("New connection", "remote", clientConn.RemoteAddr())

	// Read the startup message to extract compute information
	// We need to buffer it so we can replay it to the compute
	startupMsg, computeID, err := p.extractComputeID(clientConn)
	if err != nil {
		p.logger.Error("Failed to extract compute ID", "error", err)
		return
	}

	if computeID == "" {
		p.logger.Error("No compute ID found in connection")
		return
	}

	p.logger.Info("Extracted compute ID", "compute_id", computeID)

	// Ensure compute is scaled up
	if err := p.ensureComputeReady(ctx, computeID); err != nil {
		p.logger.Error("Failed to ensure compute is ready", "compute_id", computeID, "error", err)
		return
	}

	// Get compute service address
	computeAddr, err := p.getComputeAddress(ctx, computeID)
	if err != nil {
		p.logger.Error("Failed to get compute address", "compute_id", computeID, "error", err)
		return
	}

	// Connect to compute
	computeConn, err := net.DialTimeout("tcp", computeAddr, 10*time.Second)
	if err != nil {
		p.logger.Error("Failed to connect to compute", "compute_id", computeID, "address", computeAddr, "error", err)
		return
	}
	defer computeConn.Close()

	p.logger.Info("Connected to compute", "compute_id", computeID, "address", computeAddr)

	// Write the startup message to compute
	if _, err := computeConn.Write(startupMsg); err != nil {
		p.logger.Error("Failed to write startup message to compute", "error", err)
		return
	}

	// Proxy the connection
	p.proxyConnection(clientConn, computeConn)
}

// extractComputeID reads the PostgreSQL startup message and extracts compute ID
// Returns the full startup message (for replay) and the compute ID
func (p *Proxy) extractComputeID(conn net.Conn) ([]byte, string, error) {
	// Set read deadline
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Read startup message length (4 bytes)
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, "", fmt.Errorf("failed to read message length: %w", err)
	}

	msgLen := int32(lenBuf[0])<<24 | int32(lenBuf[1])<<16 | int32(lenBuf[2])<<8 | int32(lenBuf[3])
	msgLen -= 4 // Subtract the length field itself

	if msgLen <= 0 || msgLen > 10000 {
		return nil, "", fmt.Errorf("invalid message length: %d", msgLen)
	}

	// Read the message
	msgBuf := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, msgBuf); err != nil {
		return nil, "", fmt.Errorf("failed to read message: %w", err)
	}

	// Remove read deadline for subsequent reads
	conn.SetReadDeadline(time.Time{})

	// Parse startup message to find database/user parameters
	computeID := p.parseStartupMessage(msgBuf)

	// Reconstruct full message (length + body)
	fullMsg := make([]byte, 4+msgLen)
	copy(fullMsg[0:4], lenBuf)
	copy(fullMsg[4:], msgBuf)

	return fullMsg, computeID, nil
}

// parseStartupMessage extracts compute ID from PostgreSQL startup message
// PostgreSQL startup message format: null-terminated key-value pairs
// We look for "database" parameter which should contain the compute/branch name
func (p *Proxy) parseStartupMessage(msg []byte) string {
	// Parse null-terminated key-value pairs
	i := 0
	var currentKey string
	var currentValue string

	for i < len(msg) {
		// Find key
		keyStart := i
		for i < len(msg) && msg[i] != 0 {
			i++
		}
		if i >= len(msg) {
			break
		}
		currentKey = string(msg[keyStart:i])
		i++ // Skip null terminator

		// Find value
		valueStart := i
		for i < len(msg) && msg[i] != 0 {
			i++
		}
		if i >= len(msg) {
			break
		}
		currentValue = string(msg[valueStart:i])
		i++ // Skip null terminator

		// Check if this is the database parameter (which contains compute ID)
		if currentKey == "database" {
			return currentValue
		}

		// Also check options for compute_id parameter
		if currentKey == "options" {
			// Parse options string (format: "-c key=value -c key2=value2")
			// Look for compute_id
			// This is simplified - in production use proper parsing
			if len(currentValue) > 0 {
				// For now, assume database name is the compute ID
				// In production, you might want to use a mapping or extract from options
			}
		}
	}

	return ""
}

// ensureComputeReady checks if compute is scaled up and wakes it if needed
func (p *Proxy) ensureComputeReady(ctx context.Context, computeID string) error {
	// Find deployment by compute ID
	deployment, err := p.findDeploymentByComputeID(ctx, computeID)
	if err != nil {
		return fmt.Errorf("failed to find deployment: %w", err)
	}

	// Check if scaled down
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas == 0 {
		p.logger.Info("Compute is scaled down, waking up", "compute_id", computeID)

		// Call wake endpoint
		if err := p.wakeCompute(ctx, computeID); err != nil {
			return fmt.Errorf("failed to wake compute: %w", err)
		}

		// Wait for deployment to be ready
		if err := p.waitForDeploymentReady(ctx, deployment); err != nil {
			return fmt.Errorf("failed to wait for deployment ready: %w", err)
		}
	} else {
		// Check if pods are ready
		if err := p.waitForDeploymentReady(ctx, deployment); err != nil {
			return fmt.Errorf("deployment not ready: %w", err)
		}
	}

	return nil
}

// findDeploymentByComputeID finds a deployment by compute ID annotation
func (p *Proxy) findDeploymentByComputeID(ctx context.Context, computeID string) (*appsv1.Deployment, error) {
	deploymentList := &appsv1.DeploymentList{}

	err := p.k8sClient.List(ctx, deploymentList, client.MatchingLabels{
		"molnett.org/component": "compute",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list deployments: %w", err)
	}

	for i := range deploymentList.Items {
		deployment := &deploymentList.Items[i]
		if annotations := deployment.GetAnnotations(); annotations != nil {
			if id, exists := annotations["neon.compute_id"]; exists && id == computeID {
				return deployment, nil
			}
		}
	}

	return nil, fmt.Errorf("deployment with compute_id %s not found", computeID)
}

// wakeCompute calls the control plane wake endpoint
func (p *Proxy) wakeCompute(ctx context.Context, computeID string) error {
	url := fmt.Sprintf("%s/compute/api/v2/computes/%s/wake", p.controlPlane, computeID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call wake endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("wake endpoint returned status %d", resp.StatusCode)
	}

	p.logger.Info("Successfully woke compute", "compute_id", computeID)
	return nil
}

// waitForDeploymentReady waits for deployment to have ready replicas
func (p *Proxy) waitForDeploymentReady(ctx context.Context, deployment *appsv1.Deployment) error {
	timeout := 60 * time.Second
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for deployment to be ready")
			}

			// Refresh deployment status
			key := types.NamespacedName{
				Name:      deployment.Name,
				Namespace: deployment.Namespace,
			}
			current := &appsv1.Deployment{}
			if err := p.k8sClient.Get(ctx, key, current); err != nil {
				if errors.IsNotFound(err) {
					return fmt.Errorf("deployment not found")
				}
				continue
			}

			// Check if deployment is ready
			if current.Spec.Replicas != nil && *current.Spec.Replicas > 0 {
				if current.Status.ReadyReplicas >= 1 {
					// Also check if pod is ready
					if err := p.checkPodReady(ctx, current); err == nil {
						p.logger.Info("Deployment is ready", "compute_id", deployment.Name)
						return nil
					}
				}
			}
		}
	}
}

// checkPodReady checks if at least one pod is ready
func (p *Proxy) checkPodReady(ctx context.Context, deployment *appsv1.Deployment) error {
	podList := &corev1.PodList{}
	labels := client.MatchingLabels(deployment.Spec.Selector.MatchLabels)

	if err := p.k8sClient.List(ctx, podList, labels, client.InNamespace(deployment.Namespace)); err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning {
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					return nil
				}
			}
		}
	}

	return fmt.Errorf("no ready pods found")
}

// getComputeAddress gets the service address for a compute
// Service name pattern: {branch-name}-postgres (where branch-name is the compute ID)
func (p *Proxy) getComputeAddress(ctx context.Context, computeID string) (string, error) {
	// Find the service for this compute
	serviceList := &corev1.ServiceList{}
	err := p.k8sClient.List(ctx, serviceList, client.MatchingLabels{
		"molnett.org/component": "compute-postgres",
	})
	if err != nil {
		return "", fmt.Errorf("failed to list services: %w", err)
	}

	// Service name pattern: {branch-name}-postgres
	// The compute ID is the branch name
	expectedServiceName := fmt.Sprintf("%s-postgres", computeID)
	for _, svc := range serviceList.Items {
		if svc.Name == expectedServiceName {
			// Get service address
			port := int32(55433) // Default PostgreSQL port
			if len(svc.Spec.Ports) > 0 {
				port = svc.Spec.Ports[0].Port
			}
			return fmt.Sprintf("%s.%s.svc.cluster.local:%d", svc.Name, svc.Namespace, port), nil
		}
	}

	return "", fmt.Errorf("service for compute %s not found (expected: %s-postgres)", computeID, computeID)
}

// proxyConnection proxies data between client and compute connections
func (p *Proxy) proxyConnection(clientConn, computeConn net.Conn) {
	errChan := make(chan error, 2)

	// Copy from client to compute
	go func() {
		_, err := io.Copy(computeConn, clientConn)
		errChan <- err
	}()

	// Copy from compute to client
	go func() {
		_, err := io.Copy(clientConn, computeConn)
		errChan <- err
	}()

	// Wait for one direction to finish
	<-errChan
}
