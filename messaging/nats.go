package messaging

import (
	"context"
	"time"

	"cms/src/errors"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

// NewNatsClient creates new nats server Connection
func NewNatsClient(url string) (*nats.Conn, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}

	return nc, nil
}

// nats/stan message
type message struct {
	natsMessage *nats.Msg
	stanMessage *stan.Msg
}

func (m *message) Ack() error {
	if m.natsMessage != nil {
		return m.natsMessage.Ack()
	}
	return m.stanMessage.Ack()
}

func (m *message) Data() ([]byte, error) {
	if m.natsMessage != nil {
		return m.natsMessage.Data, nil
	}
	return m.stanMessage.Data, nil
}

type jetMsg struct {
	m       *nats.Msg
	repo    EventRepository
	eventID string
}

func (message *jetMsg) Ack() error {
	err := message.repo.Update(context.TODO(), message.eventID, true, time.Now())
	if err != nil {
		return errors.Error{Code: errors.EINTERNAL, Err: err}
	}
	return message.m.Ack()
}

func (message *jetMsg) Data() ([]byte, error) {
	return message.m.Data, nil
}
