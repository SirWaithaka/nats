package messaging

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"cms/src/domain/events"
	"cms/src/errors"

	"github.com/gofrs/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
)

const (
	usersStream = "users"
)

const (
	maxAge       = 1000000000 * 60 * 60 * 24 * 30 * 12 * 10
	maxBytes     = -1
	maxMsgs      = -1
	maxMsgSize   = -1
	maxConsumers = -1
)

func createStream(conn *nats.Conn) error {
	js, err := conn.JetStream()
	if err != nil {
		return errors.Error{Code: errors.EINTERNAL, Err: err}
	}

	// check if the users stream already exists, if not, create it.
	stream, err := js.StreamInfo(usersStream)
	if err != nil {
		return errors.Error{Code: errors.EINTERNAL, Err: err}
	}

	if stream == nil {
		log.Printf("creating stream %q ", usersStream)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:         usersStream,
			Subjects:     []string{strings.TrimSuffix(usersStream, "s")},
			MaxAge:       maxAge,
			MaxBytes:     maxBytes,
			MaxConsumers: maxConsumers,
			MaxMsgs:      maxMsgs,
			MaxMsgSize:   maxMsgSize,
		})
		if err != nil {
			return errors.Error{Code: errors.EINTERNAL, Err: err}
		}
	}
	return nil
}

type JetstreamPublisher struct {
	nc         *nats.Conn
	eventsRepo EventRepository
}

func (pub JetstreamPublisher) Publish(data events.Event) error {
	// convert data into json byte slice
	byt, err := jsoniter.Marshal(data)
	if err != nil {
		return errors.Error{Code: errors.EINTERNAL, Err: err}
	}

	// convert byte slice into map
	var mData map[string]interface{}
	err = jsoniter.Unmarshal(byt, &mData)
	if err != nil {
		return errors.Error{Code: errors.EINTERNAL, Err: err}
	}

	// generate a jetstream message key and append to data map
	uid, _ := uuid.NewV4()
	natsMessageKey := fmt.Sprintf("%v_%v", uid, time.Now().UTC().Unix())
	mData["natsJetstreamMessageKey"] = natsMessageKey

	// convert data back to byte slice
	byt, err = jsoniter.Marshal(mData)
	if err != nil {
		return errors.Error{Code: errors.EINTERNAL, Err: err}
	}

	js, err := pub.nc.JetStream()
	if err != nil {
		return errors.Error{Code: errors.EINTERNAL, Err: err}
	}

	pAck, err := js.Publish(data.Subject(), byt, nats.MsgId(natsMessageKey))
	if err != nil {
		return errors.Error{Code: errors.EINTERNAL, Err: err}
	}

	evt := EventMeta{
		Stream:         pAck.Stream,
		Sequence:       int(pAck.Sequence),
		Duplicate:      pAck.Duplicate,
		Subject:        data.Subject(),
		IsProcessed:    true,
		Action:         "published",
		NatsMessageKey: natsMessageKey,
		Data:           string(byt),
		CreatedAt:      time.Now(),
		ProcessedAt:    time.Now(),
	}
	_, err = pub.eventsRepo.Add(context.TODO(), evt)

	return nil
}

func NewJetstreamPublisher(client *nats.Conn, eventsRepo EventRepository) JetstreamPublisher {
	return JetstreamPublisher{client, eventsRepo}
}

type JetstreamSubscriber struct {
	nc         *nats.Conn
	eventsRepo EventRepository
}

func (sub JetstreamSubscriber) Subscribe(subject, queueGroupName string, onMessage func(events.Message)) error {
	opts := []nats.SubOpt{
		nats.ManualAck(),
		nats.AckExplicit(),
		// nats.AckWait(),
		nats.Durable(fmt.Sprintf("%s:%s", queueGroupName, strings.Join(strings.Split(subject, "."), "_"))),
	}

	js, err := sub.nc.JetStream()
	if err != nil {
		return errors.Error{Code: errors.EINTERNAL, Err: err}
	}

	_, err = js.Subscribe(subject, func(m *nats.Msg) {
		log.Printf("Received message #: %s\n", string(m.Data))

		// copy slice of data slice
		var data []byte
		copy(data, m.Data)

		// convert to map
		var mData map[string]interface{}
		err := jsoniter.Unmarshal(data, &mData)
		if err != nil {
			return
		}

		var natsMessageKey string
		if key, ok := mData["natsJetstreamMessageKey"]; !ok {
			return
		} else {
			natsMessageKey = key.(string)
		}

		// if a message is already redelivered, check if it is already processed
		messageMeta, err := m.Metadata()
		if err != nil {
			return
		}
		if messageMeta.NumDelivered > 1 {

			// find if message is recorded
			meta, err := sub.eventsRepo.FindByNatsKey(context.TODO(), natsMessageKey)
			if err != nil {
				return
			}
			if meta.IsProcessed {
				err := sub.eventsRepo.Update(context.TODO(), meta.ID, true, time.Now())
				if err != nil {
					return
				}
				_ = m.Ack()
			}

		}

		event := EventMeta{
			Stream: messageMeta.Stream,
			// Sequence:       messageMeta.,
			Duplicate:      false,
			Subject:        m.Subject,
			IsProcessed:    false,
			Action:         "received",
			NatsMessageKey: natsMessageKey,
			// Key:            messageMeta.Sequence.Stream,
			Data:      data,
			CreatedAt: time.Now(),
		}
		result, err := sub.eventsRepo.Add(context.TODO(), event)
		if err != nil {
			return
		}

		onMessage(&jetMsg{
			m:       m,
			repo:    sub.eventsRepo,
			eventID: result.ID,
		})

	}, opts...)
	if err != nil {
		return err
	}

	return nil
}

func NewJetstreamSubscriber(natsClient *nats.Conn, eventsRepo EventRepository) (JetstreamSubscriber, error) {
	//err := createStream(natsClient)
	//if err != nil {
	//	return JetstreamSubscriber{}, err
	//}

	return JetstreamSubscriber{natsClient, eventsRepo}, nil
}
