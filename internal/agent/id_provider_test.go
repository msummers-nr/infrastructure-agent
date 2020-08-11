// Copyright 2020 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package agent

import (
	"fmt"
	"github.com/newrelic/infrastructure-agent/pkg/integrations/v4/protocol"
	"github.com/stretchr/testify/mock"
	"math/rand"
	"testing"
	"time"

	"github.com/newrelic/infrastructure-agent/pkg/backend/inventoryapi"
	"github.com/newrelic/infrastructure-agent/pkg/backend/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"net/http"

	"github.com/newrelic/infrastructure-agent/pkg/backend/identityapi"
	"github.com/newrelic/infrastructure-agent/pkg/entity"
)

type EmptyRegisterClient struct{}

func (e *EmptyRegisterClient) RegisterEntitiesRemoveMe(agentEntityID entity.ID, entities []identityapi.RegisterEntity) (r []identityapi.RegisterEntityResponse, retryAfter time.Duration, err error) {
	return
}

func (e *EmptyRegisterClient) RegisterBatchEntities(agentEntityID entity.ID, entities []protocol.Entity) (r []identityapi.RegisterEntityResponse, retryAfter time.Duration, err error) {
	return
}

func (e *EmptyRegisterClient) RegisterEntity(agentEntityID entity.ID, entity protocol.Entity) (resp identityapi.RegisterEntityResponse, err error) {
	return
}

type incrementalRegister struct {
	state state.Register
}

func newIncrementalRegister() identityapi.RegisterClient {
	return &incrementalRegister{state: state.RegisterHealthy}
}

func newRetryAfterRegister() identityapi.RegisterClient {
	return &incrementalRegister{state: state.RegisterRetryAfter}
}

func newRetryBackoffRegister() identityapi.RegisterClient {
	return &incrementalRegister{state: state.RegisterRetryBackoff}
}

func (i *incrementalRegister) RegisterBatchEntities(agentEntityID entity.ID, entities []protocol.Entity) (batchResponse []identityapi.RegisterEntityResponse, t time.Duration, err error) {
	return
}

func (i *incrementalRegister) RegisterEntitiesRemoveMe(agentEntityID entity.ID, entities []identityapi.RegisterEntity) (responseKeys []identityapi.RegisterEntityResponse, retryAfter time.Duration, err error) {
	if i.state == state.RegisterRetryAfter {
		retryAfter = 1 * time.Second
		err = inventoryapi.NewIngestError("ingest service rejected the register step", http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError), "")
		return
	} else if i.state == state.RegisterRetryBackoff {
		err = inventoryapi.NewIngestError("ingest service rejected the register step", http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError), "")
		return
	}

	var id entity.ID
	for _, e := range entities {
		id++
		responseKeys = append(responseKeys, identityapi.RegisterEntityResponse{ID: id, Key: e.Key})
	}

	return
}

func (i *incrementalRegister) RegisterEntity(agentEntityID entity.ID, ent protocol.Entity) (identityapi.RegisterEntityResponse, error) {
	return identityapi.RegisterEntityResponse{
		ID:  entity.ID(rand.Int63n(100000)),
		Key: entity.Key(ent.Name),
	}, nil
}

type mockRegisterClient struct{ mock.Mock }

func (m *mockRegisterClient) RegisterEntitiesRemoveMe(agentEntityID entity.ID, entities []identityapi.RegisterEntity) (r []identityapi.RegisterEntityResponse, retryAfter time.Duration, err error) {
	return
}

func (m *mockRegisterClient) RegisterBatchEntities(agentEntityID entity.ID, entities []protocol.Entity) (r []identityapi.RegisterEntityResponse, retryAfter time.Duration, err error) {
	return
}

func (m *mockRegisterClient) RegisterEntity(agentEntityID entity.ID, entity protocol.Entity) (resp identityapi.RegisterEntityResponse, err error) {
	return
}

func TestNewProvideIDs_Legacy(t *testing.T) {
	provideIDs := NewProvideIDs(newIncrementalRegister(), state.NewRegisterSM(), nil)

	ids, err := provideIDs.legacy(agentIdn, registerEntities)
	assert.NoError(t, err)

	require.Len(t, ids, 1)
	assert.Equal(t, registerEntities[0].Key, ids[0].Key)
	assert.Equal(t, entity.ID(1), ids[0].ID, "incremental register should return 1 as first id")
}

func TestRetryAfter_Legacy(t *testing.T) {
	p := newIDProvider(newRetryAfterRegister(), state.NewRegisterSM(), nil)

	_, err := p.legacy(agentIdn, registerEntities)
	assert.Error(t, err)
	assert.Equal(t, state.RegisterRetryAfter, p.state.State())
}

func TestRetryBackoff_Legacy(t *testing.T) {
	p := newIDProvider(newRetryBackoffRegister(), state.NewRegisterSM(), nil)

	_, err := p.legacy(agentIdn, registerEntities)
	assert.Error(t, err)
	assert.Equal(t, state.RegisterRetryBackoff, p.state.State())
}

func TestNewProvideIDs_MemoryFirst(t *testing.T) {
	agentIdn = entity.Identity{ID: 13}
	testCases := []struct {
		cache         *EntityIDClientResponse
		agentIdentity entity.Identity
		entities      []protocol.Entity
		want          []identityapi.RegisterEntityResponse
	}{
		{
			cache: &EntityIDClientResponse{
				"remote_entity_flex": {
					ID:   6543,
					Key:  "remote_entity_flex_Key",
					Name: "remote_entity_flex",
				},
				"remote_entity_nginx": {
					ID:   1234,
					Key:  "remote_entity_nginx_Key",
					Name: "remote_entity_nginx",
				},
			},
			agentIdentity: agentIdn,
			entities: []protocol.Entity{
				{Name: "remote_entity_flex"},
				{Name: "remote_entity_nginx"},
			},
			want: []identityapi.RegisterEntityResponse{
				{
					ID:   1234,
					Key:  "remote_entity_nginx_Key",
					Name: "remote_entity_nginx",
				},
				{
					ID:   6543,
					Key:  "remote_entity_flex_Key",
					Name: "remote_entity_flex",
				},
			},
		},
	}

	for _, test := range testCases {
		registerEntityChan := make(chan RegisterEntities, 2)
		provideIDs := NewProvideIDs(&mockRegisterClient{}, state.NewRegisterSM(), registerEntityChan)
		*provideIDs.cache = *test.cache
		registeredEntities, unregisteredEntities := provideIDs.Entities(test.agentIdentity, test.entities)

		assert.Empty(t, unregisteredEntities)
		assert.ElementsMatch(t, test.want, registeredEntities)

		select {
		case r := <-registerEntityChan:
			assert.Fail(t, fmt.Sprintf("expected to not have entities to register, but found %v", r.entities))
		default:
		}
	}
}

func TestNewProvideIDs_EntityIDNotFound(t *testing.T) {
	agentIdn = entity.Identity{ID: 13}

	testCases := []struct {
		name          string
		cache         *EntityIDClientResponse
		agentIdentity entity.Identity
		entities      []protocol.Entity
		want          []identityapi.RegisterEntityResponse
		notFounded    UnregisteredEntities
		toRegister    RegisterEntities
	}{
		{
			name:          "Non of the entities were found in memory",
			cache:         &EntityIDClientResponse{},
			agentIdentity: agentIdn,
			entities: []protocol.Entity{
				{Name: "remote_entity_flex"},
			},
			notFounded: UnregisteredEntities{
				newUnregisteredEntity(
					protocol.Entity{
						Name: "remote_entity_flex",
					}, entityNotFoundInCache, nil),
			},
			toRegister: RegisterEntities{
				agentID: agentIdn.ID,
				entities: []protocol.Entity{
					{Name: "remote_entity_flex"},
				},
			},
		},
		{
			name: "One found and other not found in memory",
			cache: &EntityIDClientResponse{
				"remote_entity_redis": {
					ID:   6666,
					Key:  "remote_entity_redis_Key",
					Name: "remote_entity_redis",
				},
			},
			agentIdentity: agentIdn,
			entities: []protocol.Entity{
				{Name: "remote_entity_redis"},
				{Name: "remote_entity_nginx"},
			},
			want: []identityapi.RegisterEntityResponse{
				{
					ID:   6666,
					Key:  "remote_entity_redis_Key",
					Name: "remote_entity_redis",
				},
			},
			notFounded: UnregisteredEntities{
				newUnregisteredEntity(
					protocol.Entity{
						Name: "remote_entity_nginx",
					}, entityNotFoundInCache, nil),
			},
			toRegister: RegisterEntities{
				agentID: agentIdn.ID,
				entities: []protocol.Entity{
					{Name: "remote_entity_nginx"},
				},
			},
		},
	}

	client := &mockRegisterClient{}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			registerEntityChan := make(chan RegisterEntities, 1)

			provideIDs := NewProvideIDs(client, state.NewRegisterSM(), registerEntityChan)
			*provideIDs.cache = *test.cache

			registeredEntities, unregisteredEntities := provideIDs.Entities(test.agentIdentity, test.entities)

			assert.ElementsMatch(t, test.want, registeredEntities)
			assert.ElementsMatch(t, test.notFounded, unregisteredEntities)

			select {
			case e := <-registerEntityChan:
				assert.Equal(t, test.toRegister, e)
			default:
				assert.Fail(t, fmt.Sprintf("expected to send entities (%v) to register", test.toRegister.entities))
			}
		})
	}
}
