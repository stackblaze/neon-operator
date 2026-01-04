package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"

	"k8s.io/client-go/tools/clientcmd"
	"oltp.molnett.org/neon-operator/api/v1alpha1"
	"oltp.molnett.org/neon-operator/internal/proxy"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func run(ctx context.Context, w io.Writer, args []string) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(w, nil))

	// Get Kubernetes client
	k8sConfig, err := clientcmd.BuildConfigFromFlags("", "")
	if err != nil {
		return fmt.Errorf("failed to build k8s config: %w", err)
	}

	// Allows the client to refresh the token when it expires
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
		k8sConfig.BearerTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	}

	k8sClient, err := client.New(k8sConfig, client.Options{})
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	if err := v1alpha1.SchemeBuilder.AddToScheme(k8sClient.Scheme()); err != nil {
		return fmt.Errorf("failed to add scheme: %w", err)
	}

	// Get configuration from environment
	listenAddr := os.Getenv("PROXY_LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = "0.0.0.0:5432"
	}

	controlPlane := os.Getenv("CONTROL_PLANE_URL")
	if controlPlane == "" {
		controlPlane = "http://neon-controlplane.neon:8081"
	}

	// Create and run proxy
	cfg := proxy.Config{
		ListenAddr:   listenAddr,
		ControlPlane: controlPlane,
		K8sClient:    k8sClient,
		Logger:       logger,
	}

	p := proxy.New(cfg)
	return p.Run(ctx)
}

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Stdout, os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
