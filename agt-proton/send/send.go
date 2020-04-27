/*
#define PN_LOCAL_UNINIT (1)

 #define PN_LOCAL_ACTIVE (2)

 #define PN_LOCAL_CLOSED (4)

 #define PN_REMOTE_UNINIT (8)

 #define PN_REMOTE_ACTIVE (16)

 #define PN_REMOTE_CLOSED (32)

 const (
	SLocalUninit  State = C.PN_LOCAL_UNINIT
	SLocalActive        = C.PN_LOCAL_ACTIVE
	SLocalClosed        = C.PN_LOCAL_CLOSED
	SRemoteUninit       = C.PN_REMOTE_UNINIT
	SRemoteActive       = C.PN_REMOTE_ACTIVE
	SRemoteClosed       = C.PN_REMOTE_CLOSED
)

typedef enum {
   PN_SND_UNSETTLED = 0,
   PN_SND_SETTLED = 1,
   PN_SND_MIXED = 2
 } pn_snd_settle_mode_t;


 const (
	SndUnsettled SndSettleMode = C.PN_SND_UNSETTLED
	SndSettled   SndSettleMode = C.PN_SND_SETTLED
	SndMixed     SndSettleMode = C.PN_SND_MIXED
)

 typedef enum {
   PN_RCV_FIRST = 0,
   PN_RCV_SECOND = 1
 } pn_rcv_settle_mode_t;


const (
	RcvFirst  RcvSettleMode = C.PN_RCV_FIRST
	RcvSecond RcvSettleMode = C.PN_RCV_SECOND
)

PN_TRACE_FRM=1 go run -tags debug . amqp://localhost:5672/queue1
*/
package main

import (
	"flag"
	"fmt"
	"jf/AMQP/logger"
	"log"
	"net"
	"net/url"
	"os"
	"strings"

	"qpid.apache.org/amqp"
	"qpid.apache.org/proton"
)

func main() {
	flag.Parse()

	if len(flag.Args()) != 2 {
		fmt.Printf("send clef fichier")
		os.Exit(1)
	}

	url, err := amqp.ParseURL("amqp://localhost:5672/routage")
	fatalIf(err)
	connect(url)
}

func connect(url *url.URL) {
	logger.Printf("main()", "Connecting to %v", url)
	connection, err := net.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	fatalIf(err)
	topic := strings.TrimPrefix(url.Path, "/")
	fatalIf(err)

	adapter := proton.NewMessagingAdapter(&handler{topic})
	adapter.AutoSettle = true //TODO: timeout et retry
	adapter.PeerCloseError = true
	engine, err := proton.NewEngine(connection, adapter)
	fatalIf(err)
	logger.Printf("main()", "Accepted connection %v", engine)
	engine.Run()
	logger.Printf("main()", "Terminated %s (%v)", engine, engine.Error())
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
