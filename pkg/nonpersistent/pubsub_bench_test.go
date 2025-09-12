package nonpersistent

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func BenchmarkSubscriber(b *testing.B) {

	ctx := context.Background()
	rc, err := redisClient(ctx)
	if err != nil {
		b.Fatal(err)
	}
	tests.BenchSubscriber(b, func(n int) (message.Publisher, message.Subscriber) {
		logger := watermill.NopLogger{}

		publisher, err := NewPublisher(ctx, rc, &DefaultMarshaller{}, logger)
		if err != nil {
			panic(err)
		}

		subscriber, err := NewSubscriber(
			ctx,
			rc,
			&DefaultMarshaller{},
			logger,
		)
		if err != nil {
			panic(err)
		}

		return publisher, subscriber
	})
}
