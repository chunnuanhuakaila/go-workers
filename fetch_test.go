package workers

import (
	"time"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func buildFetch(queue string) Fetcher {
	manager := newManager(queue, nil, 1)
	fetch := manager.fetch
	go fetch.Fetch()
	return fetch
}

func FetchSpec(c gospec.Context) {
	c.Specify("Config.Fetch", func() {
		c.Specify("it returns an instance of fetch with queue", func() {
			fetch := buildFetch("fetchQueue1")
			c.Expect(fetch.Queue(), Equals, "queue:fetchQueue1")
			fetch.Close()
		})
	})

	c.Specify("Fetch", func() {
		message, _ := NewMsg("{\"jid\":\"jid\",\"foo\":\"bar\"}")

		c.Specify("it puts messages from the queues on the messages channel", func() {
			fetch := buildFetch("fetchQueue2")

			conn := Config.Pool.Get()
			defer conn.Close()

			enqueueScript.Do(conn, "queue:fetchQueue2", ARGV_VALUE_KEY, "jid", message.ToJson())

			fetch.Ready() <- true
			message := <-fetch.Messages()

			c.Expect(message, Equals, message)

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue2"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("places in progress messages on private queue", func() {
			fetch := buildFetch("fetchQueue3")

			conn := Config.Pool.Get()
			defer conn.Close()

			enqueueScript.Do(conn, "queue:fetchQueue3", ARGV_VALUE_KEY, "jid", message.ToJson())

			fetch.Ready() <- true
			<-fetch.Messages()

			len, _ := redis.Int(conn.Do("zcard", Config.Namespace+INPROGRESS_JOBS_KEY))
			c.Expect(len, Equals, 1)

			messages, _ := redis.Strings(conn.Do("zrange", Config.Namespace+INPROGRESS_JOBS_KEY, 0, -1))
			c.Expect(messages[0], Equals, "jid")

			fetch.Close()
		})

		c.Specify("removes in progress message when acknowledged", func() {
			fetch := buildFetch("fetchQueue4")

			conn := Config.Pool.Get()
			defer conn.Close()

			enqueueScript.Do(conn, "queue:fetchQueue4", ARGV_VALUE_KEY, "jid", message.ToJson())

			fetch.Ready() <- true
			<-fetch.Messages()

			fetch.Acknowledge(&Acknowledge{message, false})

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue4:1:inprogress"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("removes in progress message when serialized differently", func() {
			json := "{\"foo\":\"bar\",\"args\":[]}"
			message, _ := NewMsg(json)

			c.Expect(json, Not(Equals), message.ToJson())

			fetch := buildFetch("fetchQueue5")

			conn := Config.Pool.Get()
			defer conn.Close()

			enqueueScript.Do(conn, "queue:fetchQueue5", ARGV_VALUE_KEY, "jid", json)

			fetch.Ready() <- true
			<-fetch.Messages()

			fetch.Acknowledge(&Acknowledge{message, false})

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue5:1:inprogress"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("refires any messages left in progress from prior instance", func() {
			message2, _ := NewMsg("{\"jid\":\"jid2\",\"foo\":\"bar2\"}")
			message3, _ := NewMsg("{\"jid\":\"jid3\",\"foo\":\"bar3\"}")

			conn := Config.Pool.Get()
			defer conn.Close()

			enqueueScript.Do(conn, "queue:fetchQueue6", ARGV_VALUE_KEY, "jid", message.ToJson())
			enqueueScript.Do(conn, "queue:fetchQueue6", ARGV_VALUE_KEY, "jid2", message3.ToJson())
			enqueueScript.Do(conn, "queue:fetchQueue6", ARGV_VALUE_KEY, "jid3", message2.ToJson())

			fetch := buildFetch("fetchQueue6")

			fetch.Ready() <- true
			c.Expect(<-fetch.Messages(), Equals, message)
			fetch.Ready() <- true
			c.Expect(<-fetch.Messages(), Equals, message3)
			fetch.Ready() <- true
			c.Expect(<-fetch.Messages(), Equals, message2)

			len, _ := redis.Int(conn.Do("zcard", Config.Namespace+INPROGRESS_JOBS_KEY))
			c.Expect(len, Equals, 3)

			fetch.Acknowledge(&Acknowledge{message, false})
			fetch.Acknowledge(&Acknowledge{message3, false})
			fetch.Acknowledge(&Acknowledge{message2, false})

			len, _ = redis.Int(conn.Do("zcard", Config.Namespace+INPROGRESS_JOBS_KEY))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("update message inprogress expired time", func() {
			fetch := buildFetch("fetchQueue7")
			message, _ := NewMsg("{\"foo\":\"bar\"}")

			conn := Config.Pool.Get()
			defer conn.Close()

			now := timeToSecondsWithNanoPrecision(time.Now().Add(10 * time.Second))
			conn.Do(
				"zadd",
				Config.Namespace+INPROGRESS_JOBS_KEY,
				now,
				message.OriginalJson(),
			)

			score, _ := redis.Float64(conn.Do("zscore", Config.Namespace+INPROGRESS_JOBS_KEY, message.OriginalJson()))
			c.Expect(score, Equals, float64(now))

			fetch.Heartbeat(message)

			score, _ = redis.Float64(conn.Do("zscore", Config.Namespace+INPROGRESS_JOBS_KEY, message.OriginalJson()))
			c.Expect(score, Not(Equals), float64(now))

			fetch.Close()
		})
	})
}
