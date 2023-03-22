package db

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/flanksource/apm-hub/api/logs"
	apiv1 "github.com/flanksource/apm-hub/api/v1"
	"github.com/flanksource/apm-hub/utils"
	"github.com/flanksource/commons/logger"
	"github.com/flanksource/duty/models"
	"github.com/google/uuid"
)

func PersistLoggingBackendCRD(crd apiv1.LoggingBackend) error {
	b := models.LoggingBackend{
		ID:     uuid.MustParse(string(crd.GetUID())),
		Name:   fmt.Sprintf("%s/%s", crd.Namespace, crd.Name),
		Source: "KubernetesCRD",
		Labels: crd.Labels,
	}
	b.Spec, _ = utils.StructToJSON(crd.Spec)

	tx := gormDB.Table("logging_backends").Save(&b)
	return tx.Error
}

func PersistLoggingBackendConfigFile(config logs.SearchConfig) error {
	host, _ := os.Hostname()
	id, err := utils.DeterministicUUID(host + config.Path)
	if err != nil {
		return fmt.Errorf("error generating uuid: %v", err)
	}
	b := models.LoggingBackend{
		ID:     id,
		Name:   fmt.Sprintf("Config:%s", config.Path),
		Source: "ConfigFile",
		// TODO: Ensure this happens
		DeletedAt: nil,
	}
	b.Spec, _ = utils.StructToJSON(apiv1.LoggingBackendSpec{Backends: config.Backends})
	tx := gormDB.Table("logging_backends").Save(&b)
	return tx.Error
}

func DeleteLoggingBackend(id string) error {
	return gormDB.Table("logging_backends").
		Where("id = ?", id).
		UpdateColumn("deleted_at", time.Now()).
		Error
}

func GetLoggingBackends() ([]logs.SearchBackend, error) {
	var dbBackends []models.LoggingBackend
	err := gormDB.Table("logging_backends").Where("deleted_at IS NULL").Find(&dbBackends).Error
	if err != nil {
		return nil, err
	}

	var backends []logs.SearchBackend
	for _, dbBackend := range dbBackends {
		var spec apiv1.LoggingBackendSpec
		err := json.Unmarshal([]byte(dbBackend.Spec), &spec)
		if err != nil {
			logger.Errorf("")
			continue
		}
		backends = append(backends, spec.Backends.ToSearchBackends()...)
	}
	return backends, nil
}

func DeleteOldConfigFileBackends() error {
	return gormDB.Table("logging_backends").
		Where("source = ?", "ConfigFile").
		UpdateColumn("deleted_at", time.Now()).
		Error
}
