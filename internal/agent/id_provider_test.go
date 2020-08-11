// Copyright 2020 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package agent

import (
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
	provideIDs := NewProvideIDs(newIncrementalRegister(), state.NewRegisterSM())

	ids, err := provideIDs.legacy(agentIdn, registerEntities)
	assert.NoError(t, err)

	require.Len(t, ids, 1)
	assert.Equal(t, registerEntities[0].Key, ids[0].Key)
	assert.Equal(t, entity.ID(1), ids[0].ID, "incremental register should return 1 as first id")
}

func TestRetryAfter_Legacy(t *testing.T) {
	p := newIDProvider(newRetryAfterRegister(), state.NewRegisterSM())

	_, err := p.legacy(agentIdn, registerEntities)
	assert.Error(t, err)
	assert.Equal(t, state.RegisterRetryAfter, p.state.State())
}

func TestRetryBackoff_Legacy(t *testing.T) {
	p := newIDProvider(newRetryBackoffRegister(), state.NewRegisterSM())

	_, err := p.legacy(agentIdn, registerEntities)
	assert.Error(t, err)
	assert.Equal(t, state.RegisterRetryBackoff, p.state.State())
}

func TestNewProvideIDs_MemoryFirst(t *testing.T) {
	agentIdn = entity.Identity{ID: 13}
	testCases := []struct {
		setup         map[string]identityapi.RegisterEntityResponse
		agentIdentity entity.Identity
		entities      []protocol.Entity
		want          []identityapi.RegisterEntityResponse
	}{
		{
			setup: map[string]identityapi.RegisterEntityResponse{
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

	client := &mockRegisterClient{}

	for _, test := range testCases {
		provideIDs := NewProvideIDs(client, state.NewRegisterSM())
		provideIDs.cache = test.setup
		registeredEntities, err := provideIDs.entities(test.agentIdentity, test.entities)
		require.NoError(t, err)
		assert.ElementsMatch(t, test.want, registeredEntities)
		client.AssertNotCalled(t, "RegisterEntity")
		client.AssertNotCalled(t, "RegisterBatchEntities")
	}
}

func TestNewProvideIDs_EntityIDNotFound(t *testing.T) {
	agentIdn = entity.Identity{ID: 13}

	expectedError := func(entities []protocol.Entity) string {
		errEntityIDsNotFound := newErrEntityIDsNotFound(entities)
		return errEntityIDsNotFound.Message()
	}

	testCases := []struct {
		name          string
		setup         map[string]identityapi.RegisterEntityResponse
		agentIdentity entity.Identity
		entities      []protocol.Entity
		want          []identityapi.RegisterEntityResponse
		error         string
	}{
		{
			name:          "Non of the entities were found in memory",
			agentIdentity: agentIdn,
			entities: []protocol.Entity{
				{Name: "remote_entity_flex"},
			},
			error: expectedError([]protocol.Entity{
				{Name: "remote_entity_flex"},
			}),
		},
		{
			name: "One found and other not found in memory",
			setup: map[string]identityapi.RegisterEntityResponse{
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
			error: expectedError([]protocol.Entity{
				{Name: "remote_entity_nginx"},
			}),
		},
	}

	client := &mockRegisterClient{}

	for _, test := range testCases {
		provideIDs := NewProvideIDs(client, state.NewRegisterSM())
		provideIDs.cache = test.setup
		registeredEntities, err := provideIDs.entities(test.agentIdentity, test.entities)
		require.EqualError(t, err, test.error)
		assert.ElementsMatch(t, test.want, registeredEntities)
		client.AssertCalled(t, "RegisterBatchEntities")
	}
}
