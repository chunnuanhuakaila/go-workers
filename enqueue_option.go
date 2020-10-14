package workers

import "time"

const (
	DEFAULT_MAX_RETRIES = 25
)

type EnqueueOptions interface {
	Apply(param *EnqueueParam)
}

type EnqueueOptFunc func(param *EnqueueParam)

func (e EnqueueOptFunc) Apply(param *EnqueueParam) {
	e(param)
}

func WithIn(in time.Duration) EnqueueOptFunc {
	return (EnqueueOptFunc)(func(param *EnqueueParam) {
		param.At = nowToSecondsWithNanoPrecision() + durationToSecondsWithNanoPrecision(in)
	})
}

func WithAt(at time.Time) EnqueueOptFunc {
	return (EnqueueOptFunc)(func(param *EnqueueParam) {
		param.At = timeToSecondsWithNanoPrecision(at)
	})
}

func WithRetry() EnqueueOptFunc {
	return (EnqueueOptFunc)(func(param *EnqueueParam) {
		param.MaxRetries = DEFAULT_MAX_RETRIES
	})
}

func WithMaxRetries(maxRetries int) EnqueueOptFunc {
	return (EnqueueOptFunc)(func(param *EnqueueParam) {
		param.MaxRetries = maxRetries
	})
}

func defaultEnqueueOpt(param *EnqueueParam) {
	param.At = nowToSecondsWithNanoPrecision()
	param.MaxRetries = 0
	param.RetryCount = 0
}
