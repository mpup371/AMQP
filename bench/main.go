package main

import (
	"flag"
	"fmt"
	"log"

	"qpid.apache.org/electron"
)

var ipPort = flag.String("ip", ":61000", "<ip>:<port>")
var serverMode = flag.Bool("server", false, "mode serveur")
var clientMode = flag.Bool("client", false, "mode client")
var nbMessages = flag.Int("msg", 1000, "nombre de messages à envoyer")
var sync = flag.Bool("sync", false, "Mode synchrone")

func main() {
	flag.Parse()

	container := electron.NewContainer("jfbus")
	log.Printf("Container created : %s\n", container.Id())

	if *serverMode {
		log.Println("starting in server mode")
		runServer(container)
	} else if *clientMode {
		log.Println("starting in client mode")
		runClient(container)
	} else {
		flag.PrintDefaults()
	}

}

func PrintLink(r electron.LinkSettings) {
	fmt.Printf("Connection: %s\n", r.Session().Connection())
	fmt.Printf("LinkName: %s, Source: %s, Target: %s \n", r.LinkName(), r.Source(), r.Target())
	fmt.Printf("SndSettleMode: %+v, RcvSettleMode: %+v\n", r.SndSettle(), r.RcvSettle())
}
