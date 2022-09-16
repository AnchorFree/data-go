package schema

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/anchorfree/data-go/pkg/consul"
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/anchorfree/data-go/pkg/types"
)

type SchemaManager struct {
	mx     sync.Mutex
	config *Config
	schema *openapi3.Swagger

	validateTopics map[string]bool
}

func NewSchemaManager(config Config) *SchemaManager {
	sm := &SchemaManager{
		config:         &config,
		validateTopics: make(map[string]bool, len(config.ValidateTopics)),
	}
	for _, item := range sm.config.ValidateTopics {
		sm.validateTopics[item] = true
	}
	logger.Get().Infof("topics for validation: %#v", sm.validateTopics)
	return sm
}

func (sm *SchemaManager) Validate(event types.Event) (bool, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(event.Message, &data); err != nil {
		return false, err
	}
	for key, value := range sm.schema.Components.Schemas {
		if data[sm.config.PropertyName] == key {
			err := value.Value.VisitJSON(data)
			if err != nil {
				logger.Get().Debugf("failed validation for schema event type: %#v", key)
				return false, err
			}
			logger.Get().Debugf("successful validation for schema event type %#v", key)
			return true, nil
		}
	}
	logger.Get().Debugf("no schema for event")
	return false, nil
}

func (sm *SchemaManager) IsTopicValidated(topic string) {

}

func (sm *SchemaManager) ApplySwagger(schema *openapi3.Swagger) {
	sm.mx.Lock()
	defer sm.mx.Unlock()
	sm.schema = schema
}

func (sm *SchemaManager) RunConfigWatcher() error {
	client, err := consul.NewClient(sm.config.ConsulAddress)
	if err != nil {
		return err
	}
	watcher := consul.NewWatcher(client, nil)
	watcher.Watch(sm.config.ConsulKeyPath, sm.updateConfig)
	return nil
}

func (sm *SchemaManager) updateConfig(rawConfig []byte) error {
	swagger, err := openapi3.NewSwaggerLoader().LoadSwaggerFromData(rawConfig)
	if err != nil {
		return err
	}

	if err := swagger.Validate(context.Background()); err != nil {
		return err
	}
	sm.ApplySwagger(swagger)
	logger.Get().Info("OpenAPI schema has been successfully updated")
	return nil
}

func (sm *SchemaManager) GetInvalidMessagesTopic() string {
	if len(sm.config.InvalidMessagesTopic) > 0 {
		return sm.config.InvalidMessagesTopic
	}
	return "malformed"
}
