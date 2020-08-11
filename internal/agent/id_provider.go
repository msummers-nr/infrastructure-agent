// Copyright 2020 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package agent

import (
	"github.com/newrelic/infrastructure-agent/pkg/integrations/v4/protocol"
	"time"

	"github.com/newrelic/infrastructure-agent/pkg/log"
	"github.com/sirupsen/logrus"

	"github.com/newrelic/infrastructure-agent/pkg/backend/identityapi"
	"github.com/newrelic/infrastructure-agent/pkg/backend/state"
	"github.com/newrelic/infrastructure-agent/pkg/entity"
)

var clog = log.WithComponent("IDProvider")

// ProvideIDs provides remote entity IDs.
// Waits for next retry if register endpoint status is not healthy.
type ProvideIDs struct {
	cache *ProvideIDsNameToResponse
	legacy func(agentIdn entity.Identity, entities []identityapi.RegisterEntity) ([]identityapi.RegisterEntityResponse, error)
	Entities func(agentIdn entity.Identity, entities []protocol.Entity) (registerEntity []identityapi.RegisterEntityResponse, failedEntities ErrorStateEntities)
	ListenForNewEntities func()
}

type ErrEntityIDsNotFound struct {
	entities []protocol.Entity
}

type errorState string

const entityNotFoundInCache = "entity not found in cache"

type ErrStateEntity struct {
	State errorState
	Err error
	Entity protocol.Entity
}

type ErrorStateEntities []ErrStateEntity

func newErrorStateEntity(entity protocol.Entity, state errorState, err error) ErrStateEntity{
	return ErrStateEntity{
		State: state,
		Err: err,
		Entity: entity,
	}
}

type ProvideIDsNameToResponse map[string]identityapi.RegisterEntityResponse

type idProvider struct {
	client identityapi.RegisterClient
	state  state.RegisterSM
	cache  ProvideIDsNameToResponse
	registerEntityChan chan RegisterBatchEntities
}

type RegisterBatchEntities struct {
	entityID entity.ID
	entities []protocol.Entity
}

func (p *idProvider) entities(agentIdn entity.Identity, entities []protocol.Entity) (registeredEntity []identityapi.RegisterEntityResponse, errorStateEntities ErrorStateEntities) {

	if len(p.cache) <= 0 {
		errorStateEntities = make(ErrorStateEntities, len(entities))

		for i := range entities{
			errorStateEntities[i] = newErrorStateEntity(entities[i], entityNotFoundInCache, nil)
		}

		p.registerEntityChan <- RegisterBatchEntities{agentIdn.ID, entities}

		return nil, errorStateEntities
	}

	errorStateEntities = make(ErrorStateEntities, 0)
	entitiesToRegister := make([]protocol.Entity, 0)
	registeredEntity = make([]identityapi.RegisterEntityResponse, 0)

	for _, entity := range entities {
		if foundEntity, ok := p.cache[entity.Name]; ok {
			registeredEntity = append(registeredEntity, foundEntity)
		}else{
			errorStateEntities = append(errorStateEntities, newErrorStateEntity(entity, entityNotFoundInCache, nil))
			entitiesToRegister = append(entitiesToRegister, entity)
		}
	}

	p.registerEntityChan <- RegisterBatchEntities{agentIdn.ID, entitiesToRegister}

	return registeredEntity, errorStateEntities
}

func (p *idProvider) listenForNewEntitiesToRegister(){

	select {
	case registerBatchEntities := <-p.registerEntityChan:
		response, _, _ := p.client.RegisterBatchEntities(
			registerBatchEntities.entityID,
			registerBatchEntities.entities)

		for i := range response {
			p.cache[response[i].Name] = response[i]
		}
	}
}

// NewProvideIDs creates a new remote entity IDs provider.
func NewProvideIDs(
	client identityapi.RegisterClient,
	sm state.RegisterSM,
	registerEntityChan chan RegisterBatchEntities,
) ProvideIDs {
	p := newIDProvider(client, sm, registerEntityChan)
	return ProvideIDs{
		cache: &p.cache,
		legacy: p.legacy,
		Entities: p.entities,
		ListenForNewEntities: p.listenForNewEntitiesToRegister,
	}
}

func newIDProvider(client identityapi.RegisterClient, sm state.RegisterSM, registerEntityChan chan RegisterBatchEntities) *idProvider {
	cache := make(ProvideIDsNameToResponse)

	return &idProvider{
		client: client,
		state:  sm,
		cache: cache,
		registerEntityChan: registerEntityChan,
	}
}

// provideIDs requests ID to register endpoint, waiting for retries on failures.
// Updates the entity Map and adds the entityId if we already have them.
func (p *idProvider) legacy(agentIdn entity.Identity, entities []identityapi.RegisterEntity) (ids []identityapi.RegisterEntityResponse, err error) {
retry:
	s := p.state.State()
	if s != state.RegisterHealthy {
		after := p.state.RetryAfter()
		clog.WithFields(logrus.Fields{
			"state":      s.String(),
			"retryAfter": after,
		}).Warn("unhealthy register state. Retry requested")
		time.Sleep(after)
		goto retry
	}

	var retryAfter time.Duration
	ids, retryAfter, err = p.client.RegisterEntitiesRemoveMe(agentIdn.ID, entities)
	if err != nil {
		clog.WithFields(logrus.Fields{
			"agentID":    agentIdn,
			"retryAfter": retryAfter,
		}).Warn("cannot register entities, retry requested")
		if retryAfter > 0 {
			p.state.NextRetryAfter(retryAfter)
		} else {
			p.state.NextRetryWithBackoff()
		}
		return
	}
	return
}
