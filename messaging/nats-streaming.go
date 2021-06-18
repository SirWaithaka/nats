package messaging

import (
	"log"
	"time"

	"cms/src/domain/events"
	"cms/src/errors"

	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

// NewStanClient creates a new server connection to nats streaming server
func NewStanClient(nc *nats.Conn, clusterID, clientID string) (stan.Conn, error) {
	sc, err := stan.Connect(clusterID, clientID, stan.NatsConn(nc))
	if err != nil {
		return nil, err
	}

	return sc, nil
}

// StanServerPublisher implements the publish methods to nats streaming
type StanServerPublisher struct {
	sc stan.Conn
}

func (pub StanServerPublisher) Publish(data events.Event) error {

	byt, err := jsoniter.Marshal(data)
	if err != nil {
		return errors.Error{Code: errors.EINTERNAL, Err: err}
	}

	err = pub.sc.Publish(data.Subject(), byt)
	if err != nil {
		return errors.Error{Code: errors.EINTERNAL, Err: err}
	}
	return nil
}

func NewStanServerPublisher(natsServer stan.Conn) StanServerPublisher {
	return StanServerPublisher{natsServer}
}

// StanServerSubscriber implements the subscribe methods to nats streaming
type StanServerSubscriber struct {
	sc      stan.Conn
	ackWait time.Duration
}

func (sub StanServerSubscriber) Subscribe(subject, queueGroupName string, onMessage func(events.Message)) error {
	_, err := sub.sc.QueueSubscribe(subject, queueGroupName, func(m *stan.Msg) {
		log.Printf("Received message #: %s\n", string(m.Data))
		onMessage(&message{stanMessage: m})

	}, stan.AckWait(sub.ackWait), stan.DeliverAllAvailable(), stan.SetManualAckMode(), stan.DurableName(queueGroupName))
	if err != nil {
		return err
	}

	return nil
}

func NewStanServerSubscriber(natsClient stan.Conn) StanServerSubscriber {
	ackWait := time.Millisecond * 30 * 1000 // set ackWait to 30 seconds
	return StanServerSubscriber{sc: natsClient, ackWait: ackWait}
}
