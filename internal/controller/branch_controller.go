/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	neonv1alpha1 "oltp.molnett.org/neon-operator/api/v1alpha1"
	"oltp.molnett.org/neon-operator/utils"
)

// BranchReconciler reconciles a Branch object
type BranchReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=neon.oltp.molnett.org,resources=branches,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=neon.oltp.molnett.org,resources=branches/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=neon.oltp.molnett.org,resources=branches/finalizers,verbs=update
// +kubebuilder:rbac:groups=neon.oltp.molnett.org,resources=projects,verbs=get;list;watch
// +kubebuilder:rbac:groups=neon.oltp.molnett.org,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

func (r *BranchReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	log.Info("Reconcile loop start", "request", req)
	defer func() {
		log.Info("Reconcile loop end", "request", req)
	}()

	branch, err := r.getBranch(ctx, req)
	if err != nil || branch == nil {
		return ctrl.Result{}, err
	}

	ctx = context.WithValue(ctx, utils.BranchNameKey, branch.Name)

	result, err := r.reconcile(ctx, branch)
	if errors.Is(err, ErrRequeueAfterChange) {
		return result, nil
	} else if err != nil {
		log.Error(err, "Reconcile failed")
		return ctrl.Result{}, err
	}

	return result, nil
}

func (r *BranchReconciler) getBranch(ctx context.Context, req ctrl.Request) (*neonv1alpha1.Branch, error) {
	log := logf.FromContext(ctx)
	branch := &neonv1alpha1.Branch{}
	if err := r.Get(ctx, req.NamespacedName, branch); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Branch has been deleted")
			return nil, nil
		}

		return nil, fmt.Errorf("cannot get the resource: %w", err)
	}
	return branch, nil
}

func (r *BranchReconciler) reconcile(ctx context.Context, branch *neonv1alpha1.Branch) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if branch.Status.Phase == "" {
		if err := utils.SetPhases(ctx, r.Client, branch, utils.SetBranchCreatingStatus); err != nil {
			return ctrl.Result{}, fmt.Errorf("error setting default Status: %w", err)
		}
		log.Info("Branch phase set to creating")
	}

	// Generate timeline_id if not set
	if branch.Spec.TimelineID == "" {
		if err := r.updateTimelineID(ctx, branch); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update timelineID: %w", err)
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Get the project for this branch
	project, err := r.getProject(ctx, branch.Spec.ProjectID, branch.Namespace)
	if err != nil {
		log.Error(err, "failed to get project", "projectID", branch.Spec.ProjectID)
		return ctrl.Result{}, err
	}

	// Create timeline on storage controller
	if err := r.ensureTimeline(ctx, branch, project); err != nil {
		log.Error(err, "failed to ensure timeline")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Create branch resources
	if err := r.createBranchResources(ctx, branch, project); err != nil {
		log.Error(err, "error while creating branch resources")
		if setErr := utils.SetPhases(ctx, r.Client, branch, utils.SetBranchCannotCreateResourcesStatus); setErr != nil {
			log.Error(setErr, "failed to set branch status")
		}
		return ctrl.Result{}, fmt.Errorf("not able to create branch resources: %w", err)
	}

	// Set branch to ready status after successful resource creation
	if err := utils.SetPhases(ctx, r.Client, branch, utils.SetBranchReadyStatus); err != nil {
		log.Error(err, "failed to set branch ready status")
		return ctrl.Result{}, fmt.Errorf("failed to update branch status to ready: %w", err)
	}
	log.Info("Branch status set to ready")

	return ctrl.Result{}, nil
}

func (r *BranchReconciler) updateTimelineID(ctx context.Context, branch *neonv1alpha1.Branch) error {
	log := logf.FromContext(ctx)

	current := &neonv1alpha1.Branch{}
	if err := r.Get(ctx, types.NamespacedName{Name: branch.GetName(), Namespace: branch.GetNamespace()}, current); err != nil {
		return err
	}

	timelineID := utils.GenerateNeonID()
	updated := current.DeepCopy()
	updated.Spec.TimelineID = timelineID
	updated.ManagedFields = nil

	if err := r.Patch(ctx, updated, client.MergeFrom(current), &client.PatchOptions{FieldManager: "neon-operator"}); err != nil {
		return err
	}

	branch.Spec.TimelineID = timelineID
	log.Info("Generated and set timelineID", "timelineID", timelineID)
	return nil
}

func (r *BranchReconciler) getProject(ctx context.Context, projectID string, namespace string) (*neonv1alpha1.Project, error) {
	project := &neonv1alpha1.Project{}
	namespacedName := types.NamespacedName{
		Name:      projectID,
		Namespace: namespace,
	}

	if err := r.Get(ctx, namespacedName, project); err != nil {
		return nil, fmt.Errorf("failed to get project %s: %w", projectID, err)
	}

	return project, nil
}

func (r *BranchReconciler) getCluster(ctx context.Context, clusterName, namespace string) (*neonv1alpha1.Cluster, error) {
	cluster := &neonv1alpha1.Cluster{}
	namespacedName := types.NamespacedName{
		Name:      clusterName,
		Namespace: namespace,
	}

	if err := r.Get(ctx, namespacedName, cluster); err != nil {
		return nil, fmt.Errorf("failed to get cluster %s: %w", clusterName, err)
	}

	return cluster, nil
}

// getParentBranchTimelineID retrieves the timeline ID of a parent branch by name
func (r *BranchReconciler) getParentBranchTimelineID(ctx context.Context, parentBranchName, projectID, namespace string) (string, error) {
	parentBranch := &neonv1alpha1.Branch{}
	namespacedName := types.NamespacedName{
		Name:      parentBranchName,
		Namespace: namespace,
	}

	if err := r.Get(ctx, namespacedName, parentBranch); err != nil {
		return "", fmt.Errorf("failed to get parent branch %s: %w", parentBranchName, err)
	}

	// Verify the parent branch belongs to the same project
	if parentBranch.Spec.ProjectID != projectID {
		return "", fmt.Errorf("parent branch %s belongs to project %s, but current branch belongs to project %s", parentBranchName, parentBranch.Spec.ProjectID, projectID)
	}

	if parentBranch.Spec.TimelineID == "" {
		return "", fmt.Errorf("parent branch %s does not have a timeline ID yet", parentBranchName)
	}

	return parentBranch.Spec.TimelineID, nil
}

// getLSNByTimestamp retrieves the LSN for a given timestamp from a timeline
// Returns the LSN as a hex string
func (r *BranchReconciler) getLSNByTimestamp(ctx context.Context, clusterName, tenantID, timelineID, timestamp string) (string, error) {
	log := logf.FromContext(ctx)

	// Call the storage controller API for get_lsn_by_timestamp
	// Note: The storage controller may forward this to the pageserver
	url := fmt.Sprintf(
		"http://%s-storage-controller:8080/v1/tenant/%s/timeline/%s/get_lsn_by_timestamp?timestamp=%s",
		clusterName,
		tenantID,
		timelineID,
		timestamp,
	)

	log.Info("Getting LSN by timestamp", "url", url, "timeline_id", timelineID, "timestamp", timestamp)

	httpClient := &http.Client{
		Timeout: 30 * time.Second, // Longer timeout for this operation
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to call get_lsn_by_timestamp: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Error(err, "failed to close response body")
		}
	}()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("get_lsn_by_timestamp returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result struct {
		LSN  string `json:"lsn"`
		Kind string `json:"kind"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	log.Info("Got LSN by timestamp", "lsn", result.LSN, "kind", result.Kind)
	return result.LSN, nil
}

func (r *BranchReconciler) ensureTimeline(ctx context.Context, branch *neonv1alpha1.Branch, project *neonv1alpha1.Project) error {
	log := logf.FromContext(ctx)

	storageControllerURL := fmt.Sprintf(
		"http://%s-storage-controller:8080/v1/tenant/%s/timeline",
		project.Spec.ClusterName,
		project.Spec.TenantID,
	)

	var requestBody map[string]interface{}

	// Determine if this is a branch (has parent) or bootstrap (no parent)
	if branch.Spec.ParentBranchID == "" {
		// Bootstrap mode: create a new timeline from scratch (backward compatible)
		log.Info("Creating bootstrap timeline", "timeline_id", branch.Spec.TimelineID)
		requestBody = map[string]interface{}{
			"new_timeline_id": branch.Spec.TimelineID,
			"pg_version":      branch.Spec.PGVersion,
		}
	} else {
		// Branch mode: create a new timeline from a parent branch
		log.Info("Creating branch timeline from parent", "parent_branch_id", branch.Spec.ParentBranchID, "timeline_id", branch.Spec.TimelineID)

		// Get the parent branch's timeline ID
		parentTimelineID, err := r.getParentBranchTimelineID(ctx, branch.Spec.ParentBranchID, branch.Spec.ProjectID, branch.Namespace)
		if err != nil {
			return fmt.Errorf("failed to get parent branch timeline ID: %w", err)
		}

		branchMode := map[string]interface{}{
			"ancestor_timeline_id": parentTimelineID,
		}

		// Determine the ancestor_start_lsn
		var ancestorStartLSN string
		if branch.Spec.ParentLSN != "" {
			// Use explicit LSN if provided
			ancestorStartLSN = branch.Spec.ParentLSN
			log.Info("Using explicit parent LSN", "lsn", ancestorStartLSN)
		} else if branch.Spec.ParentTimestamp != "" {
			// Convert timestamp to LSN
			lsn, err := r.getLSNByTimestamp(ctx, project.Spec.ClusterName, project.Spec.TenantID, parentTimelineID, branch.Spec.ParentTimestamp)
			if err != nil {
				return fmt.Errorf("failed to get LSN by timestamp: %w", err)
			}
			ancestorStartLSN = lsn
			log.Info("Converted timestamp to LSN", "timestamp", branch.Spec.ParentTimestamp, "lsn", ancestorStartLSN)
		}
		// If neither ParentLSN nor ParentTimestamp is set, branch from the current head (no ancestor_start_lsn)

		if ancestorStartLSN != "" {
			branchMode["ancestor_start_lsn"] = ancestorStartLSN
		}

		// Construct the request body for Branch mode
		// The API uses a flattened structure where the mode fields are at the top level
		requestBody = map[string]interface{}{
			"new_timeline_id": branch.Spec.TimelineID,
			"ancestor_timeline_id": branchMode["ancestor_timeline_id"],
		}
		if lsn, ok := branchMode["ancestor_start_lsn"].(string); ok && lsn != "" {
			requestBody["ancestor_start_lsn"] = lsn
		}
		// pg_version is optional in Branch mode (inherits from parent), but we can set it
		requestBody["pg_version"] = branch.Spec.PGVersion
	}

	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	log.Info("Sending request to storage controller", "url", storageControllerURL, "body", string(bodyBytes))

	httpClient := &http.Client{
		Timeout: 30 * time.Second, // Increased timeout for branch operations
	}

	resp, err := httpClient.Post(storageControllerURL, "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		log.Info("Failed to connect to storage controller, will retry", "url", storageControllerURL, "error", err)
		return fmt.Errorf("failed to connect to storage controller: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Error(err, "failed to close response body")
		}
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Info("Failed to create timeline on storage controller", "status", resp.StatusCode, "response", string(bodyBytes))
		return fmt.Errorf("failed to create timeline on storage controller, status: %d: %s", resp.StatusCode, string(bodyBytes))
	}

	log.Info("Successfully created timeline on storage controller")
	return nil
}

// Resource creation functions moved to branch_create.go

// SetupWithManager sets up the controller with the Manager.
func (r *BranchReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&neonv1alpha1.Branch{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Named("branch").
		Complete(r)
}
