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
	"time"

	"github.com/flanksource/apm-hub/db"
	"github.com/flanksource/apm-hub/pkg"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	apmhubv1 "github.com/flanksource/apm-hub/api/v1"
	"github.com/go-logr/logr"
)

// LoggingBackendReconciler reconciles a LoggingBackend object
type LoggingBackendReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
}

const LoggingBackendFinalizerName = "loggingbackend.apm-hub.flanksource.com"

// +kubebuilder:rbac:groups=apm-hub.flanksource.com,resources=loggingbackends,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apm-hub.flanksource.com,resources=loggingbackends/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apm-hub.flanksource.com,resources=loggingbackends/finalizers,verbs=update
func (r *LoggingBackendReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log.WithValues("apmhub_config", req.NamespacedName)

	config := &apmhubv1.LoggingBackend{}
	err := r.Get(ctx, req.NamespacedName, config)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Error(err, "LoggingBackend not found")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Check if it is deleted, remove config
	if !config.DeletionTimestamp.IsZero() {
		logger.Info("Deleting logging backend", "id", config.GetUID())
		if err := db.DeleteLoggingBackend(string(config.GetUID())); err != nil {
			logger.Error(err, "failed to delete logging backend")
			return ctrl.Result{Requeue: true, RequeueAfter: 2 * time.Minute}, err
		}

		if err := pkg.LoadGlobalBackends(); err != nil {
			logger.Error(err, "failed to update global backends")
		}
		controllerutil.RemoveFinalizer(config, LoggingBackendFinalizerName)
		return ctrl.Result{}, r.Update(ctx, config)
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(config, LoggingBackendFinalizerName) {
		logger.Info("adding finalizer", "finalizers", config.GetFinalizers())
		controllerutil.AddFinalizer(config, LoggingBackendFinalizerName)
		if err := r.Update(ctx, config); err != nil {
			logger.Error(err, "failed to update finalizers")
		}
	}

	err = db.PersistLoggingBackendCRD(*config)
	if err != nil {
		logger.Error(err, "failed to persist logging backend")
		return ctrl.Result{}, err
	}

	err = pkg.LoadGlobalBackends()
	if err != nil {
		logger.Error(err, "failed to persist logging backend")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *LoggingBackendReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&apmhubv1.LoggingBackend{}).
		Complete(r)
}
