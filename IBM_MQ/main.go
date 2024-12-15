package main

import (
	"fmt"
	"os"
	"time"

	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

// IBM MQ connection details
const (
	queueManager = "QM1"             // Replace with your Queue Manager name
	queueName    = "DEV.QUEUE.1"     // Replace with your Queue name
	channel      = "DEV.APP.SVRCONN" // Replace with your MQ Channel name
	connection   = "localhost(1414)" // Replace with Hostname and Port (e.g., mqhost(1414))
	user         = "mqUser"          // Optional: MQ username
	password     = "mqPassword"      // Optional: MQ password
)

func main() {
	// Connect to MQ
	fmt.Println("Connecting to IBM MQ...")
	mqcd := ibmmq.NewMQCD()
	mqcd.ChannelName = channel
	mqcd.ConnectionName = connection

	// MQCNO (Connection Options)
	cno := ibmmq.NewMQCNO()
	cno.ClientConn = mqcd
	cno.Options = ibmmq.MQCNO_CLIENT_BINDING

	// User Authentication (if needed)
	if user != "" {
		sco := ibmmq.NewMQSCO()
		sco.KeyRepository = ""
		cno.SecurityParms = ibmmq.NewMQCSP()
		cno.SecurityParms.AuthenticationType = ibmmq.MQCSP_AUTH_USER_ID_AND_PWD
		cno.SecurityParms.UserId = user
		cno.SecurityParms.Password = password
	}

	// Connect to Queue Manager
	qMgr, err := ibmmq.Connx(queueManager, cno)
	if err != nil {
		fmt.Println("Error connecting to queue manager:", err)
		os.Exit(1)
	}
	defer qMgr.Disc()
	fmt.Println("Connected to Queue Manager:", queueManager)

	// Open the Queue
	od := ibmmq.NewMQOD()
	od.ObjectType = ibmmq.MQOT_Q
	od.ObjectName = queueName

	openOptions := ibmmq.MQOO_INPUT_AS_Q_DEF | ibmmq.MQOO_FAIL_IF_QUIESCING
	queue, err := qMgr.Open(od, openOptions)
	if err != nil {
		fmt.Println("Error opening queue:", err)
		os.Exit(1)
	}
	defer queue.Close(0)
	fmt.Println("Listening for messages on queue:", queueName)

	// Start Reading Messages
	for {
		// Set up the MQMD and MQGMO structures
		mqmd := ibmmq.NewMQMD()
		gmo := ibmmq.NewMQGMO()
		gmo.Options = ibmmq.MQGMO_WAIT | ibmmq.MQGMO_CONVERT
		gmo.WaitInterval = 3 * 1000 // Wait 3 seconds for a new message

		// Buffer to hold the message
		buffer := make([]byte, 1024)

		// Get message from the queue
		err = queue.Get(mqmd, gmo, buffer)
		if err != nil {
			if err.MQRC == ibmmq.MQRC_NO_MSG_AVAILABLE {
				// No message found, continue waiting
				fmt.Println("No messages available, waiting...")
				time.Sleep(1 * time.Second)
				continue
			}
			// Log other errors
			fmt.Println("Error reading message:", err)
			break
		}

		// Print the message
		message := string(buffer[:mqmd.MsgLength])
		fmt.Println("Received Message:", message)
	}
}
