package mq

import (
	"errors"
	mq_http_sdk "github.com/aliyunmq/mq-http-go-sdk"
	mq_errors "github.com/gogap/errors"
	"strings"
)

type consumerBuilder struct {
	instanceID    string
	endpoint      string
	accessKey     string
	secretKey     string
	securityToken string
	topic         string
	groupID       string
	listen        map[string]Handler
	numOfMessages int32 // 一次最多消费3条（最多可设置为16条）
	waitSeconds   int64 // 长轮询时间3秒（最多可设置为30秒）
	errHandler    func(mq_errors.ErrCode)
}

func ConsumerBuilder() *consumerBuilder {
	return &consumerBuilder{
		listen:        map[string]Handler{},
		numOfMessages: 1,
		waitSeconds:   5,
	}
}

func (cfg *consumerBuilder) InstanceID(value string) *consumerBuilder {
	cfg.instanceID = value
	return cfg
}

func (cfg *consumerBuilder) Endpoint(value string) *consumerBuilder {
	cfg.endpoint = value
	return cfg
}

func (cfg *consumerBuilder) AccessKey(value string) *consumerBuilder {
	cfg.accessKey = value
	return cfg
}

func (cfg *consumerBuilder) SecretKey(value string) *consumerBuilder {
	cfg.secretKey = value
	return cfg
}

func (cfg *consumerBuilder) SecurityToken(value string) *consumerBuilder {
	cfg.securityToken = value
	return cfg
}

func (cfg *consumerBuilder) Topic(value string) *consumerBuilder {
	cfg.topic = value
	return cfg
}

func (cfg *consumerBuilder) GroupID(value string) *consumerBuilder {
	cfg.groupID = value
	return cfg
}

func (cfg *consumerBuilder) BatchSize(size int32) *consumerBuilder {
	if size < 0 {
		return cfg
	}
	if size > 16 {
		size = 16
	}
	cfg.numOfMessages = size
	return cfg
}

func (cfg *consumerBuilder) WaitSeconds(seconds int64) *consumerBuilder {
	if seconds < 0 {
		return cfg
	}
	if seconds > 30 {
		seconds = 30
	}
	cfg.waitSeconds = seconds
	return cfg
}

func (cfg *consumerBuilder) OnError(fn func(mq_errors.ErrCode)) *consumerBuilder {
	cfg.errHandler = fn
	return cfg
}

func (cfg *consumerBuilder) Handle(tag string, handler Handler) *consumerBuilder {
	if tag == "" {
		cfg.HandleAll(handler)
	} else {
		cfg.listen[tag] = handler
	}
	return cfg
}

func (cfg *consumerBuilder) HandleAll(handler Handler) *consumerBuilder {
	cfg.listen = map[string]Handler{
		"": handler,
	}
	return cfg
}

func (cfg *consumerBuilder) Build() (Consumer, error) {
	if len(cfg.listen) == 0 {
		return nil, errors.New("handler is not defined")
	}
	tag := cfg.parseTags()
	client := mq_http_sdk.NewAliyunMQClient(cfg.endpoint, cfg.accessKey, cfg.secretKey, cfg.securityToken)
	mqConsumer := client.GetConsumer(cfg.instanceID, cfg.topic, cfg.groupID, tag)
	return &rocketMQConsumer{
		noTag:         tag == "",
		handlers:      cfg.listen,
		numOfMessages: cfg.numOfMessages,
		waitSeconds:   cfg.waitSeconds,
		errorHandler:  cfg.errHandler,
		c:             mqConsumer,
	}, nil
}

func (cfg *consumerBuilder) parseTags() string {
	var tags []string
	for tag := range cfg.listen {
		tags = append(tags, tag)
	}
	if len(tags) == 1 {
		return tags[0]
	}
	return strings.Join(tags, "||")
}
