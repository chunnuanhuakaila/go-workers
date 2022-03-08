package workers

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Fetcher interface {
	Queue() string
	Fetch()
	Acknowledge(*Acknowledge)
	Ready() chan bool
	FinishedWork() chan bool
	Messages() chan *Msg
	Close()
	Closed() bool
	HeartbeatJob(*Msg) chan bool
	Heartbeat(*Msg)
}

type fetch struct {
	queue             string
	ready             chan bool
	finishedwork      chan bool
	messages          chan *Msg
	stop              chan bool
	exit              chan bool
	closed            chan bool
	inprogressTimeout time.Duration
	heartbeatInterval time.Duration
}

func NewFetch(queue string, messages chan *Msg, ready chan bool) Fetcher {
	return &fetch{
		queue,
		ready,
		make(chan bool),
		messages,
		make(chan bool),
		make(chan bool),
		make(chan bool),
		60 * time.Second,
		50 * time.Second,
	}
}

func (f *fetch) Queue() string {
	return f.queue
}

func (f *fetch) processOldMessages() {
	conn := Config.Pool.Get()
	defer conn.Close()

	messages, err := redis.Strings(conn.Do("lrange", f.inprogressQueue(), 0, -1))
	if err != nil {
		Logger.Println("ERR: ", err)
	}

	for _, message := range messages {
		_, err = conn.Do("lpush", f.queue, message)
		if err != nil {
			Logger.Println("ERR: ", err)
			return
		}
	}

	_, err = conn.Do("del", f.inprogressQueue())
	if err != nil {
		Logger.Println("ERR: ", err)
	}
}

func (f *fetch) Fetch() {
	f.processOldMessages()
	go func() {
		for {
			// f.Close() has been called
			if f.Closed() {
				break
			}
			<-f.Ready()
			f.tryFetchMessage()
		}
	}()

	for {
		select {
		case <-f.stop:
			// Stop the redis-polling goroutine
			close(f.closed)
			// Signal to Close() that the fetcher has stopped
			close(f.exit)
			break
		}
	}
}

func (f *fetch) tryFetchMessage() {
	conn := Config.Pool.Get()
	defer conn.Close()

	message, err := redis.String(
		popMessageScript.Do(
			conn,
			f.queue,
			Config.Namespace+INPROGRESS_JOBS_KEY,
			ARGV_VALUE_KEY,
			timeToSecondsWithNanoPrecision(time.Now().Add(f.inprogressTimeout)),
		),
	)

	if err != nil {
		// If redis returns null, the queue is empty. Just ignore the error.
		if err != redis.ErrNil {
			Logger.Println("ERR: ", err)
		}
		time.Sleep(1 * time.Second)
	} else {
		f.sendMessage(message)
	}
}

func (f *fetch) sendMessage(message string) {
	msg, err := NewMsg(message)

	if err != nil {
		Logger.Println("ERR: Couldn't create message from", message, ":", err)
		return
	}

	f.Messages() <- msg
}

func (f *fetch) Acknowledge(ack *Acknowledge) {
	conn := Config.Pool.Get()
	defer conn.Close()
	remFromZ.Do(conn, Config.Namespace+INPROGRESS_JOBS_KEY, ARGV_VALUE_KEY, ack.message.Jid(), ack.KeepData)
}

func (f *fetch) Messages() chan *Msg {
	return f.messages
}

func (f *fetch) Ready() chan bool {
	return f.ready
}

func (f *fetch) FinishedWork() chan bool {
	return f.finishedwork
}

func (f *fetch) Close() {
	f.stop <- true
	<-f.exit
}

func (f *fetch) Closed() bool {
	select {
	case <-f.closed:
		return true
	default:
		return false
	}
}

func (f *fetch) inprogressQueue() string {
	return fmt.Sprint(f.queue, ":", Config.processId, ":inprogress")
}

func (f *fetch) HeartbeatJob(msg *Msg) chan bool {
	stop := make(chan bool)
	ticker := time.NewTicker(f.heartbeatInterval)
	go func() {
		defer ticker.Stop()
	hertbeatLoop:
		for {
			select {
			case <-ticker.C:
				f.Heartbeat(msg)
			case <-stop:
				break hertbeatLoop
			}
		}
	}()
	return stop
}

var setIfExistsScript = redis.NewScript(1, `
	local val = redis.call('ZSCORE', KEYS[1], ARGV[2])
	if val ~= false then
		redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
	end
`)

func (f *fetch) Heartbeat(msg *Msg) {
	conn := Config.Pool.Get()
	defer conn.Close()
	_, err := setIfExistsScript.Do(
		conn,
		Config.Namespace+INPROGRESS_JOBS_KEY,
		timeToSecondsWithNanoPrecision(time.Now().Add(f.inprogressTimeout)),
		msg.OriginalJson(),
	)
	if err != nil {
		msg.Logger.Warningln("ERR: ", err)
	}
}

func (f *fetch) HeartbeatInterval() time.Duration {
	return f.heartbeatInterval
}
