package workers

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/chein-huang/errorc"
)

const (
	LAYOUT        = "2006-01-02 15:04:05 MST"
	maxRetriesKey = "max_retries"
	retryCountKey = "retry_count"
)

type MiddlewareRetry struct{}

func (r *MiddlewareRetry) Call(queue string, message *Msg, next func() CallResult) (result CallResult) {
	result = CallResult{true, false, nil}
	defer func() {
		if e := recover(); e != nil || result.Err != nil {
			conn := Config.Pool.Get()
			defer conn.Close()

			if ShouldRetry(message) {
				message.Set("queue", queue)
				result.KeepValue = true

				if e != nil {
					message.Set("error_message", fmt.Sprintf("%v", e))
				} else {
					message.Set("error_message", fmt.Sprintf("%v", result.Err))
				}
				retryCount := incrementRetry(message)

				waitDuration := durationToSecondsWithNanoPrecision(
					time.Duration(
						secondsToDelay(retryCount),
					) * time.Second,
				)

				_, err := enqueueAtScript.Do(
					conn,
					Config.Namespace+RETRY_KEY,
					ARGV_VALUE_KEY,
					message.Jid(),
					nowToSecondsWithNanoPrecision()+waitDuration,
					message.ToJson(),
				)

				// If we can't add the job to the retry queue,
				// then we shouldn't acknowledge the job, otherwise
				// it'll disappear into the void.
				if err != nil {
					result.Acknowledge = false
					message.Logger.Errorf("add to retry queue failed, error: %v", err)
				} else {
					message.Logger.Infof(
						"add to retry queue, retry count: %d, max retries: %d",
						message.Get(retryCountKey).MustInt(),
						message.Get(maxRetriesKey).MustInt(),
					)
				}
			}

			if e != nil {
				result.Err = errorc.Newf("%v", e)
			}
		}
	}()

	result = next()

	return
}

func ShouldRetry(message *Msg) bool {
	retry := false
	max := 0
	if param, err := message.Get(maxRetriesKey).Int(); err == nil {
		max = param
		retry = param > 0
	}

	count, _ := message.Get(retryCountKey).Int()

	return retry && count < max
}

func incrementRetry(message *Msg) (retryCount int) {
	retryCount = 1

	if count, err := message.Get(retryCountKey).Int(); err != nil {
		message.Set("failed_at", time.Now().UTC().Format(LAYOUT))
	} else {
		message.Set("retried_at", time.Now().UTC().Format(LAYOUT))
		retryCount = count + 1
	}

	message.Set(retryCountKey, retryCount)

	return
}

func secondsToDelay(count int) int {
	power := math.Pow(float64(count), 4)
	return int(power) + 15 + (rand.Intn(30) * (count + 1))
}
