package mq

import (
	"context"
	mq_http_sdk "github.com/aliyunmq/mq-http-go-sdk"
	"github.com/gogap/errors"
	"log"
	"strings"
)

type (
	Consumer interface {
		Start(context.Context)
		Stop()
	}

	Message struct {
		context.Context
		mq_http_sdk.ConsumeMessageEntry
	}

	Handler func(message Message) error

	rocketMQConsumer struct {
		noTag         bool
		handlers      map[string]Handler
		numOfMessages int32 // 一次最多消费3条（最多可设置为16条）
		waitSeconds   int64 // 长轮询时间3秒（最多可设置为30秒）
		errorHandler  func(errors.ErrCode)

		c mq_http_sdk.MQConsumer

		ctx    context.Context
		cancel context.CancelFunc
	}
)

func (c *rocketMQConsumer) handleError(errCode errors.ErrCode) {
	if c.errorHandler != nil {
		c.errorHandler(errCode)
	}
}

func (c *rocketMQConsumer) Start(ctx context.Context) {
	if c.cancel != nil {
		return
	}
	c.ctx, c.cancel = context.WithCancel(ctx)

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				log.Println("退出")
			default:
				c.consume()
			}
		}
	}()

}

func (c *rocketMQConsumer) consume() {
	endChan := make(chan int)
	respChan := make(chan mq_http_sdk.ConsumeMessageResponse)
	errChan := make(chan error)

	go func() {
		select {
		case <-c.ctx.Done():
			endChan <- 1
		case resp := <-respChan:
			// 处理业务逻辑。
			for _, v := range resp.Messages {
				go c.handleOne(v)
			}
			endChan <- 1
		case err := <-errChan:
			errCode := err.(errors.ErrCode)
			if !strings.Contains(errCode.Error(), "MessageNotExist") {
				c.handleError(errCode)
			}
			endChan <- 1
		}
	}()

	// 长轮询消费消息。
	c.c.ConsumeMessage(respChan, errChan, c.numOfMessages, c.waitSeconds)
	<-endChan
}

func (c *rocketMQConsumer) handleOne(messageEntry mq_http_sdk.ConsumeMessageEntry) {
	msg := Message{Context: c.ctx, ConsumeMessageEntry: messageEntry}
	var handler Handler
	if c.noTag {
		handler = c.handlers[""]
	} else {
		handler = c.handlers[messageEntry.MessageTag]
	}
	err := handler(msg)
	if err != nil {
		ackErr := c.c.AckMessage([]string{messageEntry.ReceiptHandle})
		if ackErr != nil {
			c.handleError(ackErr.(errors.ErrCode))
		}
	}
}

func (c *rocketMQConsumer) Stop() {
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
}
