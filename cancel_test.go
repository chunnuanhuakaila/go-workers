package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func CancelSpec(c gospec.Context) {
	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("Cancel", func() {
		conn := Config.Pool.Get()
		defer conn.Close()

		c.Specify("set a cancel signal", func() {
			err := CancelJob("testjid")
			c.Expect(err, IsNil)

			found, _ := redis.Bool(conn.Do("EXISTS", "prod:"+CANCEL_KEY+"-testjid"))
			c.Expect(found, IsTrue)
		})

		c.Specify("set a cancel signal", func() {
			err := CancelJob("testjid")
			c.Expect(err, IsNil)

			found, _ := redis.Bool(conn.Do("EXISTS", "prod:"+CANCEL_KEY+"-testjid"))
			c.Expect(found, IsTrue)
		})
	})

	Config.Namespace = was
}
