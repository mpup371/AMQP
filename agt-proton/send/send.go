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

// export AGT_URL=amqp://localhost:5672/routage
// go run ./send user.agt.routage.to=TOTOTO user.agt.routage.from=111111 user.agt.routage.file=/tmp/data
package main

import (
	"flag"
	"fmt"
	"jf/AMQP/agt-proton/attributes"
	"jf/AMQP/logger"
	"log"
	"net"
	"net/url"
	"os"
	"strings"

	"qpid.apache.org/amqp"
	"qpid.apache.org/proton"
)

var urlStr string

func usage() {
	fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
	fmt.Fprintf(flag.CommandLine.Output(), "attributes:\n")
	for attr, mandatory := range attributes.Mandatory {
		fmt.Fprintf(flag.CommandLine.Output(), "%s=<string> (mandatory:%v)\n", attr, mandatory)
	}
}

// export AGT_URL=amqp://localhost:5672/routage
func main() {
	flag.StringVar(&urlStr, "agt_url", "", "url of agt server")
	flag.Usage = usage
	flag.Parse()

	if urlStr == "" {
		urlStr = os.Getenv("AGT_URL")
	}

	if urlStr == "" {
		logger.Fatalf("main()", "agt_url not found")
		os.Exit(-1)
	}

	if url, err := amqp.ParseURL(urlStr); err != nil {
		log.Fatal(err)
	} else if err := connect(url); err != nil {
		log.Fatal(err)
	}

	if len(flag.Args()) < len(attributes.Mandatory) {
		flag.Usage()
		log.Fatal("not enough parameters")
	}

	// Vérification attributs obligatoires
	valid := make(map[string]bool)
	for k, v := range attributes.Mandatory {
		if v {
			valid[k] = false
		}
	}
	for _, p := range flag.Args() {
		s := strings.Split(p, "=")
		if len(s) != 2 {
			flag.Usage()
			log.Fatal("incorrect parameter: ", p)
		}
		valid[s[0]] = true
	}
	for k, v := range valid {
		if !v {
			log.Fatal("missing parameter ", k)
		}
	}
}

func connect(url *url.URL) error {
	logger.Printf("main()", "Connecting to %v", url)

	connection, err := net.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	if err != nil {
		return err
	}

	topic := strings.TrimPrefix(url.Path, "/")
	if err != nil {
		return err
	}

	adapter := proton.NewMessagingAdapter(&handler{topic})
	adapter.AutoSettle = true //TODO: timeout et retry
	adapter.PeerCloseError = true
	engine, err := proton.NewEngine(connection, adapter)
	if err != nil {
		return err
	}

	logger.Printf("main()", "Accepted connection %v", engine)
	engine.Run()
	logger.Printf("main()", "Terminated %s (%v)", engine, engine.Error())
	return nil
}
