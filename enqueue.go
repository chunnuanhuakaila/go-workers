package workers

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/google/uuid"
)

const (
	NanoSecondPrecision = 1000000000.0
)

var (
	ErrJidExists = fmt.Errorf("jid has already exists")
)

type EnqueueData struct {
	Queue      string      `json:"queue,omitempty"`
	Class      string      `json:"class"`
	Args       interface{} `json:"args"`
	Jid        string      `json:"jid"`
	EnqueuedAt float64     `json:"enqueued_at"`
	EnqueueParam
}

type EnqueueParam struct {
	RetryCount int     `json:"retry_count,omitempty"`
	At         float64 `json:"at,omitempty"`
	MaxRetries int     `json:"max_retries,omitempty"`
	Jid        string  `json:"jid,omitempty"`
}

func generateJid() string {
	return uuid.New().String()
}

func Enqueue(queue, class string, args interface{}, opts ...EnqueueOptions) (string, error) {
	var param EnqueueParam
	defaultEnqueueOpt(&param)
	for _, opt := range opts {
		opt.Apply(&param)
	}

	now := nowToSecondsWithNanoPrecision()
	data := EnqueueData{
		Queue:        queue,
		Class:        class,
		Args:         args,
		Jid:          param.Jid,
		EnqueuedAt:   now,
		EnqueueParam: param,
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	if now < param.At {
		err := enqueueAt(data.Jid, data.At, bytes)
		return data.Jid, err
	}

	conn := Config.Pool.Get()
	defer conn.Close()

	_, err = conn.Do("sadd", Config.Namespace+"queues", queue)
	if err != nil {
		return "", err
	}
	queue = Config.Namespace + "queue:" + queue
	var ok bool
	ok, err = redis.Bool(enqueueScript.Do(conn, queue, ARGV_VALUE_KEY, data.Jid, bytes))
	if err != nil {
		return "", err
	}

	if !ok {
		return "", ErrJidExists
	}

	return data.Jid, nil
}

func enqueueAt(jid string, at float64, bytes []byte) error {
	conn := Config.Pool.Get()
	defer conn.Close()

	ok, err := redis.Bool(enqueueAtScript.Do(
		conn,
		Config.Namespace+SCHEDULED_JOBS_KEY,
		ARGV_VALUE_KEY,
		jid, at, bytes,
	))
	if err != nil {
		return err
	}

	if !ok {
		return ErrJidExists
	}

	return nil
}

func timeToSecondsWithNanoPrecision(t time.Time) float64 {
	return float64(t.UnixNano()) / NanoSecondPrecision
}

func durationToSecondsWithNanoPrecision(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / NanoSecondPrecision
}

func nowToSecondsWithNanoPrecision() float64 {
	return timeToSecondsWithNanoPrecision(time.Now())
}

func JobExists(jid string) (bool, error) {
	conn := Config.Pool.Get()
	defer conn.Close()

	exists, err := redis.Bool(conn.Do("hexists", ARGV_VALUE_KEY, jid))
	if err != nil {
		return false, err
	}

	return exists, nil
}

func CancelJob(jid string) error {
	conn := Config.Pool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", fmt.Sprintf("%s-%s", Config.Namespace+CANCEL_KEY, jid), true, "EX", inprogressTimeout)
	if err != nil {
		return err
	}

	return nil
}
