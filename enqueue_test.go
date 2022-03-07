package workers

import (
	"encoding/json"
	"time"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func EnqueueSpec(c gospec.Context) {
	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("Enqueue", func() {
		conn := Config.Pool.Get()
		defer conn.Close()

		c.Specify("makes the queue available", func() {
			Enqueue("enqueue1", "Add", []int{1, 2})

			found, _ := redis.Bool(conn.Do("sismember", "prod:queues", "enqueue1"))
			c.Expect(found, IsTrue)
		})

		c.Specify("adds a job to the queue", func() {
			nb, _ := redis.Int(conn.Do("llen", "prod:queue:enqueue2"))
			c.Expect(nb, Equals, 0)

			Enqueue("enqueue2", "Add", []int{1, 2})

			nb, _ = redis.Int(conn.Do("llen", "prod:queue:enqueue2"))
			c.Expect(nb, Equals, 1)
		})

		c.Specify("saves the arguments", func() {
			jid, _ := Enqueue("enqueue3", "Compare", []string{"foo", "bar"})

			bytes, _ := redis.Bytes(conn.Do("hget", ARGV_VALUE_KEY, jid))
			var result map[string]interface{}
			json.Unmarshal(bytes, &result)
			c.Expect(result["class"], Equals, "Compare")

			args := result["args"].([]interface{})
			c.Expect(len(args), Equals, 2)
			c.Expect(args[0], Equals, "foo")
			c.Expect(args[1], Equals, "bar")
		})

		c.Specify("has a jid", func() {
			jid, _ := Enqueue("enqueue4", "Compare", []string{"foo", "bar"})

			bytes, _ := redis.Bytes(conn.Do("hget", ARGV_VALUE_KEY, jid))
			var result map[string]interface{}
			json.Unmarshal(bytes, &result)
			c.Expect(result["class"], Equals, "Compare")

			jid = result["jid"].(string)
			c.Expect(len(jid), Equals, 24)
		})

		c.Specify("has enqueued_at that is close to now", func() {
			jid, _ := Enqueue("enqueue5", "Compare", []string{"foo", "bar"})

			bytes, _ := redis.Bytes(conn.Do("hget", ARGV_VALUE_KEY, jid))
			var result map[string]interface{}
			json.Unmarshal(bytes, &result)
			c.Expect(result["class"], Equals, "Compare")

			ea := result["enqueued_at"].(float64)
			c.Expect(ea, Not(Equals), 0)
			c.Expect(ea, IsWithin(0.1), nowToSecondsWithNanoPrecision())
		})

		c.Specify("has max_retries and retry_count when set", func() {
			jid, _ := Enqueue("enqueue6", "Compare", []string{"foo", "bar"}, WithMaxRetries(25))

			bytes, _ := redis.Bytes(conn.Do("hget", ARGV_VALUE_KEY, jid))
			var result map[string]interface{}
			json.Unmarshal(bytes, &result)
			c.Expect(result["class"], Equals, "Compare")

			max := result["max_retries"].(float64)
			c.Expect(max, Equals, float64(25))

			c.Expect(result["retry_count"], Equals, nil)
		})
	})

	c.Specify("EnqueueIn", func() {
		scheduleQueue := "prod:" + SCHEDULED_JOBS_KEY
		conn := Config.Pool.Get()
		defer conn.Close()

		c.Specify("has added a job in the scheduled queue", func() {
			_, err := Enqueue("enqueuein1", "Compare", map[string]interface{}{"foo": "bar"}, WithIn(10*time.Millisecond))
			c.Expect(err, Equals, nil)

			scheduledCount, _ := redis.Int(conn.Do("zcard", scheduleQueue))
			c.Expect(scheduledCount, Equals, 1)

			conn.Do("del", scheduleQueue)
		})

		c.Specify("has the correct 'queue'", func() {
			jid, err := Enqueue("enqueuein2", "Compare", map[string]interface{}{"foo": "bar"}, WithIn(10*time.Millisecond))
			c.Expect(err, Equals, nil)

			var data EnqueueData
			elem, err := conn.Do("hget", ARGV_VALUE_KEY, jid)
			bytes, err := redis.Bytes(elem, err)
			json.Unmarshal(bytes, &data)

			c.Expect(data.Queue, Equals, "enqueuein2")

			conn.Do("del", scheduleQueue)
		})
	})

	Config.Namespace = was
}
