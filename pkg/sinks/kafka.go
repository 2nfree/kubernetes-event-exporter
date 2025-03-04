package sinks

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"

	"github.com/Shopify/sarama"
	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
	"github.com/rs/zerolog/log"

	"github.com/xdg-go/scram"
)

// KafkaConfig is the Kafka producer configuration
type KafkaConfig struct {
	Topic            string                 `yaml:"topic"`
	Brokers          []string               `yaml:"brokers"`
	CustomFields     map[string]interface{} `yaml:"customFields"`
	Layout           map[string]interface{} `yaml:"layout"`
	ClientId         string                 `yaml:"clientId"`
	CompressionCodec string                 `yaml:"compressionCodec" default:"none"`
	Version          string                 `yaml:"version"`
	TLS              struct {
		Enable             bool   `yaml:"enable"`
		CaFile             string `yaml:"caFile"`
		CertFile           string `yaml:"certFile"`
		KeyFile            string `yaml:"keyFile"`
		InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
	} `yaml:"tls"`
	SASL struct {
		Enable    bool   `yaml:"enable"`
		Username  string `yaml:"username"`
		Password  string `yaml:"password"`
		Mechanism string `yaml:"mechanism" default:"plain"`
	} `yaml:"sasl"`
	KafkaEncode Avro `yaml:"avro"`
}

// KafkaEncoder is an interface type for adding an
// encoder to the kafka data pipeline
type KafkaEncoder interface {
	encode([]byte) ([]byte, error)
}

// KafkaSink is a sink that sends events to a Kafka topic
type KafkaSink struct {
	producer sarama.SyncProducer
	cfg      *KafkaConfig
	encoder  KafkaEncoder
}

var CompressionCodecs = map[string]sarama.CompressionCodec{
	"none":   sarama.CompressionNone,
	"snappy": sarama.CompressionSnappy,
	"gzip":   sarama.CompressionGZIP,
	"lz4":    sarama.CompressionLZ4,
	"zstd":   sarama.CompressionZSTD,
}

func NewKafkaSink(cfg *KafkaConfig) (Sink, error) {
	var avro KafkaEncoder
	producer, err := createSaramaProducer(cfg)
	if err != nil {
		return nil, err
	}

	log.Info().Msgf("kafka: Producer initialized for topic: %s, brokers: %s", cfg.Topic, cfg.Brokers)
	if len(cfg.KafkaEncode.SchemaID) > 0 {
		var err error
		avro, err = NewAvroEncoder(cfg.KafkaEncode.SchemaID, cfg.KafkaEncode.Schema)
		if err != nil {
			return nil, err
		}
		log.Info().Msgf("kafka: Producer using avro encoding with schemaid: %s", cfg.KafkaEncode.SchemaID)
	}

	return &KafkaSink{
		producer: producer,
		cfg:      cfg,
		encoder:  avro,
	}, nil
}

// 深度合并函数，用于合并嵌套的 map 结构
func deepMerge(dst, src map[string]interface{}) map[string]interface{} {
	for key, srcVal := range src {
		if dstVal, ok := dst[key]; ok {
			// 如果目标和源都是 map，则递归合并
			srcMap, srcMapOk := srcVal.(map[string]interface{})
			dstMap, dstMapOk := dstVal.(map[string]interface{})
			if srcMapOk && dstMapOk {
				dst[key] = deepMerge(dstMap, srcMap)
				continue
			}
		}
		// 对于其他情况，直接覆盖或添加
		dst[key] = srcVal
	}
	return dst
}

func (k *KafkaSink) buildSendMessage(ev *kube.EnhancedEvent) ([]byte, error) {
	var toSend []byte

	if k.cfg.Layout != nil {
		res, err := convertLayoutTemplate(k.cfg.Layout, ev)
		if err != nil {
			return nil, err
		}

		// 如果存在自定义字段，将其深度合并到布局结果中
		if len(k.cfg.CustomFields) > 0 {
			res = deepMerge(res, k.cfg.CustomFields)
		}

		toSend, err = json.Marshal(res)
		if err != nil {
			return nil, err
		}
	} else if len(k.cfg.KafkaEncode.SchemaID) > 0 {
		// 对于 Avro 编码，我们需要先处理自定义字段，然后再进行编码
		if len(k.cfg.CustomFields) > 0 {
			// 将事件转换为 map 以便添加自定义字段
			var eventMap map[string]interface{}
			if err := json.Unmarshal(ev.ToJSON(), &eventMap); err != nil {
				return nil, err
			}

			// 深度合并自定义字段
			eventMap = deepMerge(eventMap, k.cfg.CustomFields)

			// 重新序列化
			modifiedJSON, err := json.Marshal(eventMap)
			if err != nil {
				return nil, err
			}

			// 使用修改后的 JSON 进行 Avro 编码
			toSend, err = k.encoder.encode(modifiedJSON)
			if err != nil {
				return nil, err
			}
		} else {
			// 如果没有自定义字段，使用原始逻辑
			var err error
			toSend, err = k.encoder.encode(ev.ToJSON())
			if err != nil {
				return nil, err
			}
		}
	} else {
		// 默认 JSON 处理
		if len(k.cfg.CustomFields) > 0 {
			// 将事件转换为 map 以便添加自定义字段
			var eventMap map[string]interface{}
			if err := json.Unmarshal(ev.ToJSON(), &eventMap); err != nil {
				return nil, err
			}

			// 深度合并自定义字段
			eventMap = deepMerge(eventMap, k.cfg.CustomFields)

			// 重新序列化
			var err error
			toSend, err = json.Marshal(eventMap)
			if err != nil {
				return nil, err
			}
		} else {
			toSend = ev.ToJSON()
		}
	}
	return toSend, nil
}

// Send an event to Kafka synchronously
func (k *KafkaSink) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	buf, err := k.buildSendMessage(ev)
	if err != nil {
		return err
	}
	_, _, err = k.producer.SendMessage(&sarama.ProducerMessage{
		Topic: k.cfg.Topic,
		Key:   sarama.StringEncoder(string(ev.UID)),
		Value: sarama.ByteEncoder(buf),
	})

	return err
}

// Close the Kafka producer
func (k *KafkaSink) Close() {
	log.Info().Msgf("kafka: Closing producer...")

	if err := k.producer.Close(); err != nil {
		log.Error().Err(err).Msg("Failed to shut down the Kafka producer cleanly")
	} else {
		log.Info().Msg("kafka: Closed producer")
	}
}

func createSaramaProducer(cfg *KafkaConfig) (sarama.SyncProducer, error) {
	// Default Sarama config
	saramaConfig := sarama.NewConfig()
	if cfg.Version != "" {
		version, err := sarama.ParseKafkaVersion(cfg.Version)
		if err != nil {
			return nil, err
		}
		saramaConfig.Version = version
	} else {
		saramaConfig.Version = sarama.MaxVersion
	}
	saramaConfig.Metadata.Full = true
	saramaConfig.ClientID = cfg.ClientId

	// Necessary for SyncProducer
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	if _, ok := CompressionCodecs[cfg.CompressionCodec]; ok {
		saramaConfig.Producer.Compression = CompressionCodecs[cfg.CompressionCodec]
	}

	// TLS Client auth override
	if cfg.TLS.Enable {

		caCert, err := os.ReadFile(cfg.TLS.CaFile)
		if err != nil {
			return nil, fmt.Errorf("error loading ca file: %w", err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
		}

		if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
			if err != nil {
				return nil, err
			}

			saramaConfig.Net.TLS.Config.Certificates = []tls.Certificate{cert}
		}
	}

	// SASL Client auth
	if cfg.SASL.Enable {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = cfg.SASL.Username
		saramaConfig.Net.SASL.Password = cfg.SASL.Password
		if cfg.SASL.Mechanism == "sha512" {
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		} else if cfg.SASL.Mechanism == "sha256" {
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		} else if cfg.SASL.Mechanism == "plain" || cfg.SASL.Mechanism == "" {
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		} else {
			return nil, fmt.Errorf("invalid scram sha mechanism: %s: can be one of 'sha256', 'sha512' or 'plain'", cfg.SASL.Mechanism)
		}
	}

	// TODO: Find a generic way to override all other configs

	// Build producer
	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
