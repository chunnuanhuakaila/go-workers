package workers

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

type config struct {
	processId    string
	Namespace    string
	PollInterval int
	Pool         *redis.Pool
	Fetch        func(queue string) Fetcher
}

var Config *config

func Configure(options map[string]interface{}) {
	var poolSize int
	var namespace string
	var pollInterval int

	if options["server"] == nil || options["server"].(string) == "" {
		panic("Configure requires a 'server' option, which identifies a Redis instance")
	}
	if options["process"] == nil || options["process"].(string) == "" {
		panic("Configure requires a 'process' option, which uniquely identifies this instance")
	}
	if options["pool"] == nil {
		poolSize = 1
	} else {
		poolSize, _ = options["pool"].(int)
	}
	if options["namespace"] != nil {
		namespace = options["namespace"].(string) + ":"
	}
	if seconds, ok := options["poll_interval"].(int); ok {
		pollInterval = seconds
	} else {
		pollInterval = 15
	}

	useTLS := false
	if p, ok := options["use_tls"].(bool); ok {
		useTLS = p
	}
	dialOpts := []redis.DialOption{redis.DialUseTLS(useTLS)}

	Config = &config{
		options["process"].(string),
		namespace,
		pollInterval,
		&redis.Pool{
			MaxIdle:     poolSize,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", options["server"].(string), dialOpts...)
				if err != nil {
					return nil, err
				}
				if options["password"] != nil && options["password"].(string) != "" {
					if _, err := c.Do("AUTH", options["password"].(string)); err != nil {
						c.Close()
						return nil, err
					}
				}
				if options["database"] != nil {
					if _, err := c.Do("SELECT", options["database"].(int)); err != nil {
						c.Close()
						return nil, err
					}
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
		func(queue string) Fetcher {
			return NewFetch(queue, make(chan *Msg), make(chan bool))
		},
	}
}
