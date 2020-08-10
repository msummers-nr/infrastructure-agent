// Copyright 2020 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package agent

import (
	"fmt"
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
	legacy func(agentIdn entity.Identity, entities []identityapi.RegisterEntity) ([]identityapi.RegisterEntityResponse, error)
	cache  ProvideIDsFromMemory
}

type ErrEntityIDsNotFound struct {
	entities []protocol.Entity
}

func newErrEntityIDsNotFound(entities []protocol.Entity) ErrEntityIDsNotFound {
	return ErrEntityIDsNotFound{
		entities: entities,
	}
}

func (e *ErrEntityIDsNotFound) Error() string {
	return fmt.Sprintf("could not found the following entities: %p", e.entities)
}

type ProvideIDsFromMemory []identityapi.RegisterEntityResponse

func (p *ProvideIDs) entities(agentIdn entity.Identity, entities []protocol.Entity) (ids []identityapi.RegisterEntityResponse, err error) {
	return []identityapi.RegisterEntityResponse{
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
	}, nil
}

type idProvider struct {
	client identityapi.RegisterClient
	state  state.RegisterSM
}

// NewProvideIDs creates a new remote entity IDs provider.
func NewProvideIDs(
	client identityapi.RegisterClient,
	sm state.RegisterSM,
) ProvideIDs {
	p := newIDProvider(client, sm)
	return ProvideIDs{legacy: p.legacy}
}

func newIDProvider(client identityapi.RegisterClient, sm state.RegisterSM) *idProvider {
	return &idProvider{
		client: client,
		state:  sm,
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
