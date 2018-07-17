// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package producers

import (
	"encoding/json"

	"github.com/matrix-org/dendrite/typingserver/types"
	"github.com/matrix-org/gomatrixserverlib"

	sarama "gopkg.in/Shopify/sarama.v1"
)

// TypingServerProducer produces events for the typing server to consume
type TypingServerProducer struct {
	Topic    string
	Producer sarama.SyncProducer
}

// Send typing event to kafka topic InputTypingEvent
func (p *TypingServerProducer) Send(event gomatrixserverlib.Event) error {
	data := types.InputTypingEvent{Event: event}
	value, err := json.Marshal(data)
	if err != nil {
		return err
	}

	m := &sarama.ProducerMessage{
		Topic: string(p.Topic),
		Key:   sarama.StringEncoder(event.RoomID()),
		Value: sarama.ByteEncoder(value),
	}

	_, _, err = p.Producer.SendMessage(m)
	return err
}
