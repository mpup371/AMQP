/*TODO: passer en modules */
package main

import (
	"flag"
	"fmt"
	"jf/AMQP/logger"
	"os"
	"time"

	"qpid.apache.org/electron"
)

//TODO voir si nécessaire
// const credit = 20

//TODO timeout paramétrable (et dans  broker)
const timeout = 5 * time.Second

var addr = flag.String("addr", ":amqp", "Network address to listen on, in the form \"host:port\"")

func main() {
	flag.Parse()
	var name string
	host, err := os.Hostname()
	if err == nil {
		name = fmt.Sprintf("routage(%s)", host)
	} else {
		name = fmt.Sprintf("routage(%d)", os.Getegid())
	}

	b := &broker{
		queues:    makeQueues(),
		container: electron.NewContainer(name),
	}

	if err := b.run(*addr); err != nil {
		logger.Fatalf("routage", "exit: %s", err)
	}
}
