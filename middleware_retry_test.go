package workers

import (
	"context"
	"fmt"
	"time"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func MiddlewareRetryWithPanicSpec(c gospec.Context) {
	var panicingJob = (func(message *Msg) error {
		panic("AHHHH")
	})

	var wares = NewMiddleware(
		&MiddlewareRetry{},
	)

	layout := "2006-01-02 15:04:05 MST"
	manager := newManager("myqueue", panicingJob, 1)
	worker := newWorker(manager)

	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("puts messages in retry queue when they fail", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"max_retries\":25}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		data, _ := redis.String(conn.Do("hget", ARGV_VALUE_KEY, retries[0]))
		c.Expect(data, Equals, message.ToJson())
	})

	c.Specify("allows disabling retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":false}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))
		c.Expect(count, Equals, 0)
	})

	c.Specify("doesn't retry by default", func() {
		message, _ := NewMsg("{\"jid\":\"2\"}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))
		c.Expect(count, Equals, 0)
	})

	c.Specify("allows numeric retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"max_retries\":5}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		data, _ := redis.String(conn.Do("hget", ARGV_VALUE_KEY, retries[0]))
		c.Expect(data, Equals, message.ToJson())
	})

	c.Specify("handles new failed message", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"max_retries\":25}")

		var result CallResult
		wares.call("myqueue", message, func() error {
			result = worker.process(message)
			return nil
		})
		c.Expect(result.KeepValue, Equals, true)

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		data, _ := redis.String(conn.Do("hget", ARGV_VALUE_KEY, retries[0]))
		message, _ = NewMsg(data)

		queue, _ := message.Get("queue").String()
		error_message, _ := message.Get("error_message").String()
		error_class, _ := message.Get("error_class").String()
		retry_count, _ := message.Get("retry_count").Int()
		error_backtrace, _ := message.Get("error_backtrace").String()
		failed_at, _ := message.Get("failed_at").String()

		c.Expect(queue, Equals, "myqueue")
		c.Expect(error_message, Equals, "AHHHH")
		c.Expect(error_class, Equals, "")
		c.Expect(retry_count, Equals, 1)
		c.Expect(error_backtrace, Equals, "")
		c.Expect(failed_at, Equals, time.Now().UTC().Format(layout))
	})

	c.Specify("handles recurring failed message", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"max_retries\":25,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":10}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		data, _ := redis.String(conn.Do("hget", ARGV_VALUE_KEY, retries[0]))
		message, _ = NewMsg(data)

		queue, _ := message.Get("queue").String()
		error_message, _ := message.Get("error_message").String()
		retry_count, _ := message.Get("retry_count").Int()
		failed_at, _ := message.Get("failed_at").String()
		retried_at, _ := message.Get("retried_at").String()

		c.Expect(queue, Equals, "myqueue")
		c.Expect(error_message, Equals, "AHHHH")
		c.Expect(retry_count, Equals, 11)
		c.Expect(failed_at, Equals, "2013-07-20 14:03:42 UTC")
		c.Expect(retried_at, Equals, time.Now().UTC().Format(layout))
	})

	c.Specify("handles recurring failed message with customized max", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"max_retries\":10,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":8}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		data, _ := redis.String(conn.Do("hget", ARGV_VALUE_KEY, retries[0]))
		message, _ = NewMsg(data)

		queue, _ := message.Get("queue").String()
		error_message, _ := message.Get("error_message").String()
		retry_count, _ := message.Get("retry_count").Int()
		failed_at, _ := message.Get("failed_at").String()
		retried_at, _ := message.Get("retried_at").String()

		c.Expect(queue, Equals, "myqueue")
		c.Expect(error_message, Equals, "AHHHH")
		c.Expect(retry_count, Equals, 9)
		c.Expect(failed_at, Equals, "2013-07-20 14:03:42 UTC")
		c.Expect(retried_at, Equals, time.Now().UTC().Format(layout))
	})

	c.Specify("doesn't retry after default number of retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true,\"retry_count\":25}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))
		c.Expect(count, Equals, 0)
	})

	c.Specify("doesn't retry after customized number of retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":3,\"retry_count\":3}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))
		c.Expect(count, Equals, 0)
	})

	Config.Namespace = was
}

func MiddlewareRetryWithErrorSpec(c gospec.Context) {
	var errorJob = (func(message *Msg) error {
		return fmt.Errorf("AHHHH")
	})

	var wares = NewMiddleware(
		&MiddlewareRetry{},
	)

	layout := "2006-01-02 15:04:05 MST"
	manager := newManager("myqueue", errorJob, 1)
	worker := newWorker(manager)

	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("puts messages in retry queue when they fail", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"max_retries\":25}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		data, _ := redis.String(conn.Do("hget", ARGV_VALUE_KEY, retries[0]))
		c.Expect(data, Equals, message.ToJson())
	})

	c.Specify("allows disabling retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":false}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))
		c.Expect(count, Equals, 0)
	})

	c.Specify("doesn't retry by default", func() {
		message, _ := NewMsg("{\"jid\":\"2\"}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))
		c.Expect(count, Equals, 0)
	})

	c.Specify("allows numeric retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"max_retries\":5}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		data, _ := redis.String(conn.Do("hget", ARGV_VALUE_KEY, retries[0]))
		c.Expect(data, Equals, message.ToJson())
	})

	c.Specify("handles new failed message", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"max_retries\":25}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		data, _ := redis.String(conn.Do("hget", ARGV_VALUE_KEY, retries[0]))
		message, _ = NewMsg(data)

		queue, _ := message.Get("queue").String()
		error_message, _ := message.Get("error_message").String()
		error_class, _ := message.Get("error_class").String()
		retry_count, _ := message.Get("retry_count").Int()
		error_backtrace, _ := message.Get("error_backtrace").String()
		failed_at, _ := message.Get("failed_at").String()

		c.Expect(queue, Equals, "myqueue")
		c.Expect(error_message, Equals, "AHHHH")
		c.Expect(error_class, Equals, "")
		c.Expect(retry_count, Equals, 1)
		c.Expect(error_backtrace, Equals, "")
		c.Expect(failed_at, Equals, time.Now().UTC().Format(layout))
	})

	c.Specify("handles recurring failed message", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"max_retries\":25,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":10}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		data, _ := redis.String(conn.Do("hget", ARGV_VALUE_KEY, retries[0]))
		message, _ = NewMsg(data)

		queue, _ := message.Get("queue").String()
		error_message, _ := message.Get("error_message").String()
		retry_count, _ := message.Get("retry_count").Int()
		failed_at, _ := message.Get("failed_at").String()
		retried_at, _ := message.Get("retried_at").String()

		c.Expect(queue, Equals, "myqueue")
		c.Expect(error_message, Equals, "AHHHH")
		c.Expect(retry_count, Equals, 11)
		c.Expect(failed_at, Equals, "2013-07-20 14:03:42 UTC")
		c.Expect(retried_at, Equals, time.Now().UTC().Format(layout))
	})

	c.Specify("handles recurring failed message with customized max", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"max_retries\":10,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":8}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		data, _ := redis.String(conn.Do("hget", ARGV_VALUE_KEY, retries[0]))
		message, _ = NewMsg(data)

		queue, _ := message.Get("queue").String()
		error_message, _ := message.Get("error_message").String()
		retry_count, _ := message.Get("retry_count").Int()
		failed_at, _ := message.Get("failed_at").String()
		retried_at, _ := message.Get("retried_at").String()

		c.Expect(queue, Equals, "myqueue")
		c.Expect(error_message, Equals, "AHHHH")
		c.Expect(retry_count, Equals, 9)
		c.Expect(failed_at, Equals, "2013-07-20 14:03:42 UTC")
		c.Expect(retried_at, Equals, time.Now().UTC().Format(layout))
	})

	c.Specify("doesn't retry after default number of retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true,\"retry_count\":25}")

		wares.call("myqueue", message, func() error {
			worker.process(message)
			return nil
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))
		c.Expect(count, Equals, 0)
	})

	Config.Namespace = was
}

func MiddlewareRetryWithCancelSpec(c gospec.Context) {
	var blockJob = (func(message *Msg) error {
		var err error
		for err == nil {
			err = message.Context.Err()
		}
		return err
	})

	var wares = NewMiddleware(
		&MiddlewareRetry{},
	)

	manager := newManager("myqueue", blockJob, 1)
	worker := newWorker(manager)

	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("doesn't retry after cancel the job", func() {
		message, _ := NewMsg("{\"jid\":\"1\",\"max_retries\":3,\"retry_count\":1}")
		var cancel context.CancelFunc
		message.Context, cancel = context.WithCancel(message.Context)
		processed := make(chan bool)

		go func() {
			wares.call("myqueue", message, func() error {
				worker.process(message)
				return nil
			})
			processed <- true
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()
		<-processed

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))
		c.Expect(count, Equals, 0)
	})

	c.Specify("doesn't retry when the job timeout", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"max_retries\":3,\"retry_count\":1}")
		message.Context, _ = context.WithTimeout(message.Context, 100*time.Millisecond)
		processed := make(chan bool)

		go func() {
			wares.call("myqueue", message, func() error {
				worker.process(message)
				return nil
			})
			processed <- true
		}()
		<-processed

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", Config.Namespace+RETRY_KEY))
		c.Expect(count, Equals, 1)

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		data, _ := redis.String(conn.Do("hget", ARGV_VALUE_KEY, retries[0]))
		message, _ = NewMsg(data)
		queue, _ := message.Get("queue").String()
		error_message, _ := message.Get("error_message").String()
		retry_count, _ := message.Get("retry_count").Int()

		c.Expect(queue, Equals, "myqueue")
		c.Expect(error_message, Equals, context.DeadlineExceeded.Error())
		c.Expect(retry_count, Equals, 2)
	})

	Config.Namespace = was
}
