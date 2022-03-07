package workers

import (
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

type scheduled struct {
	keys   []string
	closed chan bool
	exit   chan bool
}

func (s *scheduled) start() {
	go (func() {
		for {
			select {
			case <-s.closed:
				return
			default:
			}

			s.poll()

			time.Sleep(time.Duration(Config.PollInterval) * time.Second)
		}
	})()
}

func (s *scheduled) quit() {
	close(s.closed)
}

func (s *scheduled) poll() {
	conn := Config.Pool.Get()

	now := nowToSecondsWithNanoPrecision()

	for _, key := range s.keys {
		key = Config.Namespace + key
		for {
			messages, _ := redis.Strings(scheduledScript.Do(conn, key, ARGV_VALUE_KEY, now))
			if len(messages) == 0 {
				break
			}

			message, _ := NewMsg(messages[1])
			queue, _ := message.Get("queue").String()
			queue = strings.TrimPrefix(queue, Config.Namespace)
			message.Set("enqueued_at", nowToSecondsWithNanoPrecision())
			targetQueue := Config.Namespace + "queue:" + queue
			redis.Bool(moveToL.Do(conn, key, targetQueue, ARGV_VALUE_KEY, messages[0], message.ToJson()))
		}
	}

	conn.Close()
}

func newScheduled(keys ...string) *scheduled {
	return &scheduled{keys, make(chan bool), make(chan bool)}
}
