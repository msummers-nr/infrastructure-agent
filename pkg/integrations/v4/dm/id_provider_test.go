package dm

import (
	"fmt"
	"github.com/newrelic/infrastructure-agent/pkg/backend/identityapi"
	"github.com/newrelic/infrastructure-agent/pkg/entity"
	"github.com/newrelic/infrastructure-agent/pkg/integrations/v4/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestIdProvider_Entities_MemoryFirst(t *testing.T) {

	agentIdn := entity.Identity{ID: 13}

	registerClient := &mockedRegisterClient{}
	registerClient.
		On("RegisterBatchEntities", agentIdn.ID, mock.Anything).
		Return([]identityapi.RegisterEntityResponse{}, time.Second, nil)

	cache := RegisteredEntitiesNameIDMap{
		"remote_entity_flex":  6543,
		"remote_entity_nginx": 1234,
	}

	entities := []protocol.Entity{
		{Name: "remote_entity_flex"},
		{Name: "remote_entity_nginx"},
	}

	idProvider := NewIDProvider(registerClient)

	idProvider.cache = cache
	idProvider.Entities(agentIdn, entities)

	registerClient.AssertNotCalled(t, "RegisterBatchEntities")
}

func TestIdProvider_Entities_OneCachedAnotherRegistered(t *testing.T) {

	agentIdn := entity.Identity{ID: 13}

	entitiesForRegisterClient := []protocol.Entity{
		{
			Name: "remote_entity_nginx",
		},
	}

	registerClientResponse := []identityapi.RegisterEntityResponse{
		{
			ID:   1234,
			Key:  "remote_entity_nginx_Key",
			Name: "remote_entity_nginx",
		},
	}

	registerClient := &mockedRegisterClient{}
	registerClient.
		On("RegisterBatchEntities", mock.Anything, mock.Anything).
		Return(registerClientResponse, time.Second, nil)

	cache := RegisteredEntitiesNameIDMap{
		"remote_entity_flex": 6543,
	}

	entities := []protocol.Entity{
		{Name: "remote_entity_flex"},
		{Name: "remote_entity_nginx"},
	}

	idProvider := NewIDProvider(registerClient)

	idProvider.cache = cache
	registeredEntities, unregisteredEntities := idProvider.Entities(agentIdn, entities)

	assert.Empty(t, unregisteredEntities)
	assert.Len(t, registeredEntities, 2)

	registerClient.AssertCalled(t, "RegisterBatchEntities", agentIdn.ID, entitiesForRegisterClient)
}

func TestIdProvider_Entities_ErrorsHandling(t *testing.T) {

	testCases := []struct {
		name                         string
		agentIdn                     entity.Identity
		cache                        RegisteredEntitiesNameIDMap
		entitiesForRegisterClient    []protocol.Entity
		registerClientResponse       []identityapi.RegisterEntityResponse
		registerClientResponseErr    error
		entitiesToRegister           []protocol.Entity
		registeredEntitiesExpected   RegisteredEntitiesNameIDMap
		unregisteredEntitiesExpected UnregisteredEntities
	}{
		{
			name:     "OneCached_OneFailed_ErrClient",
			agentIdn: entity.Identity{ID: 13},
			cache: RegisteredEntitiesNameIDMap{
				"remote_entity_flex": 6543,
			},
			entitiesForRegisterClient: []protocol.Entity{
				{
					Name: "remote_entity_nginx",
				},
			},
			registerClientResponse:    []identityapi.RegisterEntityResponse{},
			registerClientResponseErr: fmt.Errorf("internal server error"),
			entitiesToRegister: []protocol.Entity{
				{Name: "remote_entity_flex"},
				{Name: "remote_entity_nginx"},
			},
			registeredEntitiesExpected: RegisteredEntitiesNameIDMap{
				"remote_entity_flex": 6543,
			},
			unregisteredEntitiesExpected: UnregisteredEntities{
				{
					Reason: reasonClientError,
					Err:    fmt.Errorf("internal server error"),
					Entity: protocol.Entity{
						Name: "remote_entity_nginx",
					},
				},
			},
		},
		{
			name:     "OneCached_OneFailed_ErrEntity",
			agentIdn: entity.Identity{ID: 13},
			cache: RegisteredEntitiesNameIDMap{
				"remote_entity_flex": 6543,
			},
			entitiesForRegisterClient: []protocol.Entity{
				{
					Name: "remote_entity_nginx",
				},
			},
			registerClientResponse: []identityapi.RegisterEntityResponse{
				{
					Key:  "remote_entity_nginx_Key",
					Name: "remote_entity_nginx",
					Err:  "invalid entityName",
				},
			},
			entitiesToRegister: []protocol.Entity{
				{Name: "remote_entity_flex"},
				{Name: "remote_entity_nginx"},
			},
			registeredEntitiesExpected: RegisteredEntitiesNameIDMap{
				"remote_entity_flex": 6543,
			},
			unregisteredEntitiesExpected: UnregisteredEntities{
				{
					Reason: reasonEntityError,
					Err:    fmt.Errorf("invalid entityName"),
					Entity: protocol.Entity{
						Name: "remote_entity_nginx",
					},
				},
			},
		},
		{
			name:     "OneCached_OneRegistered_OneFailed_ErrEntity",
			agentIdn: entity.Identity{ID: 13},
			cache: RegisteredEntitiesNameIDMap{
				"remote_entity_flex": 6543,
			},
			entitiesForRegisterClient: []protocol.Entity{
				{
					Name: "remote_entity_nginx",
				},
				{
					Name: "remote_entity_kafka",
				},
			},
			registerClientResponse: []identityapi.RegisterEntityResponse{
				{
					Key:  "remote_entity_nginx",
					Name: "remote_entity_nginx",
					Err:  "invalid entityName",
				},
				{
					ID:   1234,
					Key:  "remote_entity_kafka",
					Name: "remote_entity_kafka",
				},
			},
			entitiesToRegister: []protocol.Entity{
				{Name: "remote_entity_flex"},
				{Name: "remote_entity_nginx"},
				{Name: "remote_entity_kafka"},
			},
			registeredEntitiesExpected: RegisteredEntitiesNameIDMap{
				"remote_entity_flex":  6543,
				"remote_entity_kafka": 1234,
			},
			unregisteredEntitiesExpected: UnregisteredEntities{
				{
					Reason: reasonEntityError,
					Err:    fmt.Errorf("invalid entityName"),
					Entity: protocol.Entity{
						Name: "remote_entity_nginx",
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registerClient := &mockedRegisterClient{}
			registerClient.
				On("RegisterBatchEntities", mock.Anything, mock.Anything).
				Return(testCase.registerClientResponse, time.Second, testCase.registerClientResponseErr)

			idProvider := NewIDProvider(registerClient)

			idProvider.cache = testCase.cache
			registeredEntities, unregisteredEntities := idProvider.Entities(testCase.agentIdn, testCase.entitiesToRegister)

			assert.Equal(t, testCase.registeredEntitiesExpected, registeredEntities)

			assert.ElementsMatch(t, testCase.unregisteredEntitiesExpected, unregisteredEntities)

			registerClient.AssertCalled(t, "RegisterBatchEntities", testCase.agentIdn.ID, testCase.entitiesForRegisterClient)
		})
	}

}
