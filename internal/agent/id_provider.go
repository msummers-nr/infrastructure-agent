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
	cache                *EntityIDClientResponse
	legacy               func(agentIdn entity.Identity, entities []identityapi.RegisterEntity) ([]identityapi.RegisterEntityResponse, error)
	Entities             func(agentIdn entity.Identity, entities []protocol.Entity) (registerEntity []identityapi.RegisterEntityResponse, failedEntities UnregisteredEntities)
	ListenForNewEntities func()
}

type EntityIDClientResponse map[string]identityapi.RegisterEntityResponse

// NewProvideIDs creates a new remote entity IDs provider.
func NewProvideIDs(
	client identityapi.RegisterClient,
	sm state.RegisterSM,
	registerEntityChan chan RegisterEntities,
) ProvideIDs {
	p := newIDProvider(client, sm, registerEntityChan)
	return ProvideIDs{
		cache:                &p.cache,
		legacy:               p.legacy,
		Entities:             p.entities,
		ListenForNewEntities: p.registerEntitiesListener,
	}
}

// UnregisteredEntity contains those entities that are not registered in NR backend
//		Reason: contain the cause of why the entity was not registered
//		Err: 	error that the client could return
//		Entity: the unregistered entity
type UnregisteredEntities []UnregisteredEntity

type UnregisteredEntity struct {
	Reason reason
	Err    error
	Entity protocol.Entity
}

type reason string

const entityNotFoundInCache = "entity not found in cache"

func newUnregisteredEntity(entity protocol.Entity, reason reason, err error) UnregisteredEntity {
	return UnregisteredEntity{
		Entity: entity,
		Reason: reason,
		Err:    err,
	}
}

type RegisterEntities struct {
	agentID  entity.ID
	entities []protocol.Entity
}

type idProvider struct {
	client             identityapi.RegisterClient
	state              state.RegisterSM
	cache              EntityIDClientResponse
	registerEntityChan chan RegisterEntities
}

func newIDProvider(client identityapi.RegisterClient, sm state.RegisterSM, registerEntityChan chan RegisterEntities) *idProvider {
	cache := make(EntityIDClientResponse)

	return &idProvider{
		client:             client,
		state:              sm,
		cache:              cache,
		registerEntityChan: registerEntityChan,
	}
}

func (p *idProvider) entities(agentIdn entity.Identity, entities []protocol.Entity) (registeredEntities []identityapi.RegisterEntityResponse, unregisteredEntities UnregisteredEntities) {

	unregisteredEntities = make(UnregisteredEntities, 0)

	if len(p.cache) <= 0 {
		for _, e := range entities {
			unregisteredEntities = append(unregisteredEntities, newUnregisteredEntity(e, entityNotFoundInCache, nil))
		}

		p.registerEntityChan <- RegisterEntities{agentIdn.ID, entities}

		return nil, unregisteredEntities
	}

	entitiesToRegister := make([]protocol.Entity, 0)
	registeredEntities = make([]identityapi.RegisterEntityResponse, 0)

	for _, e := range entities {
		if found, ok := p.cache[e.Name]; ok {
			registeredEntities = append(registeredEntities, found)
		} else {
			unregisteredEntities = append(unregisteredEntities, newUnregisteredEntity(e, entityNotFoundInCache, nil))
			entitiesToRegister = append(entitiesToRegister, e)
		}
	}

	if len(entitiesToRegister) > 0 {
		p.registerEntityChan <- RegisterEntities{agentIdn.ID, entitiesToRegister}
	}

	return registeredEntities, unregisteredEntities
}

func (p *idProvider) registerEntitiesListener() {

	select {
	case registerEntity := <-p.registerEntityChan:
		response, _, _ := p.client.RegisterBatchEntities(
			registerEntity.agentID,
			registerEntity.entities)

		for i := range response {
			p.cache[response[i].Name] = response[i]
		}
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
