package mq

import "github.com/aliyunmq/mq-http-go-sdk"

type producerBuilder struct {
	instanceID    string
	endpoint      string
	accessKey     string
	secretKey     string
	securityToken string
	topic         string
	tag           string
}

func ProducerBuilder() *producerBuilder {
	return &producerBuilder{}
}

func (cfg *producerBuilder) InstanceID(value string) *producerBuilder {
	cfg.instanceID = value
	return cfg
}

func (cfg *producerBuilder) Endpoint(value string) *producerBuilder {
	cfg.endpoint = value
	return cfg
}

func (cfg *producerBuilder) AccessKey(value string) *producerBuilder {
	cfg.accessKey = value
	return cfg
}

func (cfg *producerBuilder) SecretKey(value string) *producerBuilder {
	cfg.secretKey = value
	return cfg
}

func (cfg *producerBuilder) SecurityToken(value string) *producerBuilder {
	cfg.securityToken = value
	return cfg
}

func (cfg *producerBuilder) Topic(value string) *producerBuilder {
	cfg.topic = value
	return cfg
}
func (cfg *producerBuilder) Tag(value string) *producerBuilder {
	cfg.tag = value
	return cfg
}

func (cfg *producerBuilder) Build() Producer {
	client := mq_http_sdk.NewAliyunMQClient(cfg.endpoint, cfg.accessKey, cfg.secretKey, cfg.securityToken)
	return rocketMQProducer{
		p:      client.GetProducer(cfg.instanceID, cfg.topic),
		msgTag: cfg.tag,
	}
}
