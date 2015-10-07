// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zmq

import (
	"fmt"
	"math"
	"sync"

	zmq "github.com/pebbe/zmq4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/log"
)

// Publishes Prometheus samples as zeromq messages on a PUB socket
type Publisher struct {
	listenAddr     string
	publisher      *zmq.Socket
	ignoredSamples prometheus.Counter
	mutex          sync.Mutex
}

// Create new Publisher
func NewPublisher(listenAddr string) *Publisher {
	publisher, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		panic("Error creating zeromq socket")
	}
	publisher.Bind(listenAddr)
	return &Publisher{
		listenAddr: listenAddr,
		publisher:  publisher,
		ignoredSamples: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "prometheus_zmq_ignored_samples_total",
				Help: "The total number of samples not published on zeromq due to unsupported float values (Inf, -Inf, NaN).",
			},
		),
	}
}

// Publish a batch of samples as individual zeromq messages
func (c *Publisher) Store(samples model.Samples) error {
	for _, s := range samples {
		v := float64(s.Value)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			log.Debugf("cannot send value %f. skipping sample %#v", v, s)
			c.ignoredSamples.Inc()
			continue
		}
		msg := fmt.Sprintf("%s %s %d", s.Metric, s.Value.String(), s.Timestamp)

		c.mutex.Lock()
		c.publisher.Send(msg, 0)
		c.mutex.Unlock()
	}

	return nil
}

// Name identifies the client as a ZeroMQ publisher.
func (c Publisher) Name() string {
	return "zmq"
}

// Describe implements prometheus.Collector.
func (c *Publisher) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ignoredSamples.Desc()
}

// Collect implements prometheus.Collector.
func (c *Publisher) Collect(ch chan<- prometheus.Metric) {
	ch <- c.ignoredSamples
}
