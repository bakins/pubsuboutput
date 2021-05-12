// Package pubsuboutput provides a GCP PubSub output for filebeat
package pubsuboutput

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"google.golang.org/api/option"
)

func init() {
	outputs.RegisterType("gcp-pubsub", newPubSubOutput)
}

type config struct {
	Topic           string          `config:"topic" validate:"required"`
	ProjectID       string          `config:"project_id" validate:"required"`
	CredentialsFile string          `config:"credentials_file"`
	CredentialsJSON common.JSONBlob `config:"credentials_json"`
	MaxRetries      int             `config:"max_retries"`
	BulkMaxSize     int             `config:"bulk_max_size"`
}

func defaultConfig() config {
	var c config

	c.BulkMaxSize = 4096

	return c
}

func newPubSubOutput(_ outputs.IndexManager, info beat.Info, observer outputs.Observer, cfg *common.Config) (outputs.Group, error) {
	c := defaultConfig()
	if err := cfg.Unpack(&c); err != nil {
		return outputs.Fail(err)
	}

	clients := []outputs.NetworkClient{
		&pubsubClient{
			name:     fmt.Sprintf("pubsub:%s/%s", c.ProjectID, c.Topic),
			config:   c,
			observer: observer,
		},
	}
	return outputs.SuccessNet(false, c.BulkMaxSize, c.MaxRetries, clients)
}

type pubsubClient struct {
	observer outputs.Observer
	client   *pubsub.Client
	topic    *pubsub.Topic
	name     string
	config   config
}

func (p *pubsubClient) Connect() error {
	if p.client != nil {
		return nil
	}

	var opts []option.ClientOption

	if p.config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(p.config.CredentialsFile))
	} else if len(p.config.CredentialsJSON) > 0 {
		opts = append(opts, option.WithCredentialsJSON(p.config.CredentialsJSON))
	}

	client, err := pubsub.NewClient(context.Background(), p.config.ProjectID, opts...)
	if err != nil {
		return fmt.Errorf("failed to create pubsub client %w", err)
	}

	p.client = client
	p.topic = client.Topic(p.config.Topic)

	return nil
}

func (p *pubsubClient) Close() error {
	p.topic.Stop()

	err := p.client.Close()

	p.client = nil

	return err
}

func (p *pubsubClient) String() string {
	return p.name
}

func (p *pubsubClient) Publish(ctx context.Context, batch publisher.Batch) error {
	events := batch.Events()
	p.observer.NewBatch(len(events))

	if len(events) == 0 {
		return nil
	}

	var buf bytes.Buffer

	gz := gzipPool.Get().(*gzip.Writer)
	defer gzipPool.Put(gz)
	gz.Reset(&buf)

	enc := json.NewEncoder(gz)

	for i := range events {
		e := &events[i]
		if err := enc.Encode(e); err != nil {
			p.observer.Dropped(len(events))
			batch.Drop()
			return fmt.Errorf("failed to encode event %w", err)
		}

		if _, err := buf.Write([]byte("\n")); err != nil {
			p.observer.Failed(len(events))
			batch.Retry()
			return fmt.Errorf("failed to append newline %w", err)
		}
	}

	if err := gz.Close(); err != nil {
		p.observer.Failed(len(events))
		batch.Retry()
		return fmt.Errorf("failed to compress events %w", err)
	}

	msg := pubsub.Message{
		Data: buf.Bytes(),
	}

	res := p.topic.Publish(context.Background(), &msg)
	if _, err := res.Get(context.Background()); err != nil {
		p.observer.Failed(len(events))
		batch.Retry()
	}

	batch.ACK()

	return nil
}

var gzipPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}
