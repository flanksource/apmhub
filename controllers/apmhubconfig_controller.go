/*
Copyright 2023.

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

package controllers

import (
	"context"

	logsAPI "github.com/flanksource/apm-hub/api/logs"
	"github.com/flanksource/apm-hub/pkg"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	apmhubv1 "github.com/flanksource/apm-hub/api/v1"
	"github.com/flanksource/apm-hub/utils"
	"github.com/go-logr/logr"
)

// APMHubConfigReconciler reconciles a APMHubConfig object
type APMHubConfigReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
}

const APMHUBConfigFinalizerName = "config.apm-hub.flanksource.com"

// +kubebuilder:rbac:groups=apm-hub.flanksource.com,resources=apmhubconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apm-hub.flanksource.com,resources=apmhubconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apm-hub.flanksource.com,resources=apmhubconfigs/finalizers,verbs=update
func (r *APMHubConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log.WithValues("apmhub_config", req.NamespacedName)

	config := &apmhubv1.APMHubConfig{}
	err := r.Get(ctx, req.NamespacedName, config)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Error(err, "APMHUBConfig not found")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Check if it is deleted, remove config
	if !config.DeletionTimestamp.IsZero() {
		logger.Info("Deleting config", "id", config.GetUID())
		removeBackendFromGlobalBackends(config.Spec.Backends)
		controllerutil.RemoveFinalizer(config, APMHUBConfigFinalizerName)
		return ctrl.Result{}, r.Update(ctx, config)
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(config, APMHUBConfigFinalizerName) {
		logger.Info("adding finalizer", "finalizers", config.GetFinalizers())
		controllerutil.AddFinalizer(config, APMHUBConfigFinalizerName)
		if err := r.Update(ctx, config); err != nil {
			logger.Error(err, "failed to update finalizers")
		}
	}

	for _, b := range config.Spec.Backends {
		sb := b.ToSearchBackend()
		if err := pkg.AttachSearchAPIToBackend(&sb); err != nil {
			logger.Error(err, "error adding search api to backend")
			continue
		}
		logsAPI.GlobalBackends = append(logsAPI.GlobalBackends, sb)
	}
	return ctrl.Result{}, nil
}

func removeBackendFromGlobalBackends(backends []logsAPI.SearchBackendCRD) {
	for i, gb := range logsAPI.GlobalBackends {
		for _, b := range backends {
			hashA, _ := utils.Hash(gb)
			hashB, _ := utils.Hash(b)
			if hashA == hashB {
				logsAPI.GlobalBackends = append(logsAPI.GlobalBackends[:i], logsAPI.GlobalBackends[i+1:]...)
				continue
			}
		}
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *APMHubConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&apmhubv1.APMHubConfig{}).
		Complete(r)
}
