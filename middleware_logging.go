package workers

import (
	"runtime"
	"time"
)

type MiddlewareLogging struct{}

func (l *MiddlewareLogging) Call(queue string, message *Msg, next func() CallResult) (result CallResult) {
	start := time.Now()
	message.Logger.Println("start")
	message.Logger.Println("args:", message.Args().ToJson())

	defer func() {
		if e := recover(); e != nil {
			message.Logger.Println("fail:", time.Since(start))

			buf := make([]byte, 4096)
			buf = buf[:runtime.Stack(buf, false)]
			message.Logger.Printf("error: %v\n%s", e, buf)

			panic(e)
		} else if result.Err != nil {
			message.Logger.Println("fail:", time.Since(start))
			message.Logger.Printf("error: %+v", result.Err)
		}
	}()

	result = next()

	message.Logger.Println("done:", time.Since(start))

	return
}
