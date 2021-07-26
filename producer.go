package mq

import (
	"github.com/aliyunmq/mq-http-go-sdk"
	"time"
)

type (
	Property struct {
		Key   string
		Value string
	}
	Producer interface {
		SendMsg(msg string, properties ...Property) (string, error)
		SendTagMsg(tag string, msg string, properties ...Property) (string, error)
		SendDelayMsg(msg string, delay time.Duration, properties ...Property) (string, error)
		SendDeliverMsg(msg string, deliverTime time.Time, properties ...Property) (string, error)
	}
	rocketMQProducer struct {
		msgTag string
		p      mq_http_sdk.MQProducer
	}
)

func (p rocketMQProducer) send(request mq_http_sdk.PublishMessageRequest, properties ...Property) (string, error) {
	if len(properties) > 0 {
		props := make(map[string]string, len(properties))
		for _, prop := range properties {
			if prop.Key != "" {
				props[prop.Key] = prop.Value
			}
		}
		request.Properties = props
	}
	resp, err := p.p.PublishMessage(request)
	if err != nil {
		return "", err
	}
	return resp.MessageId, err
}

func (p rocketMQProducer) SendMsg(msg string, properties ...Property) (string, error) {
	request := mq_http_sdk.PublishMessageRequest{
		MessageBody: msg,      //消息内容
		MessageTag:  p.msgTag, // 消息标签
	}
	return p.send(request, properties...)
}

func (p rocketMQProducer) SendTagMsg(tag string, msg string, properties ...Property) (string, error) {
	request := mq_http_sdk.PublishMessageRequest{
		MessageBody: msg, //消息内容
		MessageTag:  tag, // 消息标签
	}
	return p.send(request, properties...)
}

func (p rocketMQProducer) SendDelayMsg(msg string, delay time.Duration, properties ...Property) (string, error) {
	request := mq_http_sdk.PublishMessageRequest{
		MessageBody: msg,      //消息内容
		MessageTag:  p.msgTag, // 消息标签
	}
	// 设置时间
	if delay > 0 {
		request.StartDeliverTime = time.Now().Add(delay).UnixNano() / 1e6
	}
	return p.send(request, properties...)
}

func (p rocketMQProducer) SendDeliverMsg(msg string, deliverAt time.Time, properties ...Property) (string, error) {
	request := mq_http_sdk.PublishMessageRequest{
		MessageBody: msg,      //消息内容
		MessageTag:  p.msgTag, // 消息标签
	}
	if deliverAt.After(time.Now()) {
		request.StartDeliverTime = deliverAt.UnixNano() / 1e6
	}
	return p.send(request, properties...)
}
