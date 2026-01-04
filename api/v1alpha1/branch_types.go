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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BranchSpec defines the desired state of Branch
type BranchSpec struct {
	// Will be generated unless specified.
	// Has to be a 32 character alphanumeric string.
	// +optional
	TimelineID string `json:"timelineID"`

	// PGVersion specifies the PostgreSQL version to use for the branch.
	// kubebuilder:validation:Enum=14;15;16;17
	// kubebuilder:default:=17
	PGVersion int `json:"pgVersion"`

	// The ID of the Project this Branch belongs to
	ProjectID string `json:"projectID"`

	// AutoScale enables automatic scaling of compute instances to zero when idle.
	// When enabled, compute instances will scale down to 0 replicas after being idle
	// for the duration specified by SuspendTimeoutSeconds.
	// +optional
	// +kubebuilder:default:=false
	AutoScale bool `json:"autoScale,omitempty"`

	// SuspendTimeoutSeconds specifies the number of seconds of inactivity before
	// a compute instance is suspended. A value of -1 disables auto-suspend.
	// Only used when AutoScale is enabled.
	// +optional
	// +kubebuilder:default:=-1
	SuspendTimeoutSeconds *int `json:"suspendTimeoutSeconds,omitempty"`

	// ParentBranchID specifies the name of the parent branch to branch from.
	// If set, the new branch will be created from this parent branch's timeline.
	// Mutually exclusive with creating a bootstrap branch (when not set, creates a new bootstrap branch).
	// +optional
	ParentBranchID string `json:"parentBranchID,omitempty"`

	// ParentTimestamp specifies an RFC3339 timestamp for point-in-time branching.
	// If set along with ParentBranchID, the new branch will be created from the parent branch
	// at the specified point in time. The timestamp will be converted to an LSN automatically.
	// +optional
	ParentTimestamp string `json:"parentTimestamp,omitempty"`

	// ParentLSN specifies a specific LSN (Log Sequence Number) to branch from.
	// If set along with ParentBranchID, the new branch will be created from the parent branch
	// at this specific LSN. Mutually exclusive with ParentTimestamp (if both are set, ParentLSN takes precedence).
	// +optional
	ParentLSN string `json:"parentLSN,omitempty"`
}

// BranchStatus defines the observed state of Branch.
type BranchStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty,omitzero"`

	Phase string `json:"phase,omitempty"`
}

const (
	BranchPhaseCreating               = "Creating"
	BranchPhaseReady                  = "Ready"
	BranchPhaseCannotCreateResources  = "CannotCreateResources"
	BranchPhaseTimelineCreationFailed = "TimelineCreationFailed"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Branch is the Schema for the branches API
type Branch struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of Branch
	// +required
	Spec BranchSpec `json:"spec"`

	// status defines the observed state of Branch
	// +optional
	Status BranchStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// BranchList contains a list of Branch
type BranchList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Branch `json:"items"`
}

// GetConditions returns the conditions for the branch status
func (b *BranchStatus) GetConditions() []metav1.Condition {
	return b.Conditions
}

// SetConditions sets the conditions for the branch status
func (b *BranchStatus) SetConditions(conditions []metav1.Condition) {
	b.Conditions = conditions
}

// GetPhase returns the phase for the branch status
func (b *BranchStatus) GetPhase() string {
	return b.Phase
}

// SetPhase sets the phase for the branch status
func (b *BranchStatus) SetPhase(phase string) {
	b.Phase = phase
}

func init() {
	SchemeBuilder.Register(&Branch{}, &BranchList{})
}
