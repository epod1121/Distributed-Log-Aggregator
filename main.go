package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/epod1121/Log-Aggregator/.gitignore/pb"
	"google.golang.org/protobuf/proto"
)

var (
	topicOffsetMap = make(map[string]map[int]int64)
	topicOffsetCount = make(map[string]int)
	offsetMutex sync.Mutex
)

type SystemStats struct {
	sync.Mutex
	Logs int
	Added int
	Checkouts int
	Income int
	SignUps int
	LastEvent string
}

// open up producer connection
type Producer struct {
	conn net.Conn
}

func main() {
	fmt.Print("Starting Program...")

	// start broker server
	go startServer()

	// sleep to allow connection to open
	time.Sleep(100 * time.Millisecond)

	// create a shared stats instance
	stats := &SystemStats{}

	// start logs flowing
	go processLog("localhost:9092", "payment", stats)
	go processLog("localhost:9092", "add to cart", stats)
	go processLog("localhost:9092", "new sign up", stats)

	// start terminal UI
	go runUI(stats)

	// start producer
	producer, err := newLogProducer("localhost:9092")
	if err != nil {
		fmt.Println("Error starting producer")
		return
	}

	// start simulated traffic
	startSimulatedTraffic(producer)
}

// ======================================================================================
// Traffic Simulation - source of all logs that are sent through system
// ======================================================================================

func startSimulatedTraffic(producer *Producer){

	methods := []func(*Producer){addToCart, newSignUp, payment}

	for {
		// pick a random method from the list
		randomIndex := rand.Intn(len(methods))
		methods[randomIndex](producer)

		// sleep for a random time in range
		time.Sleep(time.Duration(100+rand.Intn(300)) * time.Millisecond)
	}
}

// funcs to handle simulated traffic
func addToCart(producer *Producer) {
	err := producer.send("Activity Alert", time.Now().Format(time.RFC1123), "add to cart", "An item was added to a cart!")
	if err != nil {
		fmt.Println("An error occurred sending a log")
	}
}

func newSignUp(producer *Producer) {
	err := producer.send("Sign up", time.Now().Format(time.RFC1123), "new sign up", "New user signed up")
	if err != nil {
		fmt.Println("An error occurred sending a log")
	}
}

func payment(producer *Producer) {
	randomInt := strconv.Itoa(rand.Intn(1000))
	err := producer.send("Checkout", time.Now().Format(time.RFC1123), "payment", randomInt)
	if err != nil {
		fmt.Println("An error occurred sending a log")
	}
}

// ======================================================================================
// Producer - get log messages and send them to broker
// ======================================================================================

// connect and hold open the connection to tcp address
func newLogProducer(address string) (*Producer, error){

	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("Server is offline")
		return nil, err
	}
	return &Producer{conn: conn}, nil
}

// package and ships a single log
func (p *Producer) send(from string, time string, topic string, message string) error {

	// take the time, topic, and message and turn them into protobuf
	log := &pb.Log {
		From:		from,
		Time:		time,
		Topic:		topic,
		Message:	message,
	}

	// marshal (turn into bytes) the log data
	data, err := proto.Marshal(log)
	if err != nil {
		fmt.Println("Error marshalling")
		return err
	}

	// get lengths
    topicBytes := []byte(topic)
    topicLen := make([]byte, 4)
    binary.BigEndian.PutUint32(topicLen, uint32(len(topicBytes)))

    dataLen := make([]byte, 4)
    binary.BigEndian.PutUint32(dataLen, uint32(len(data)))

    // combine everything into a single network packet:
	// 1 byte id --> 4 byte topic len --> topic --> 4 byte data len --> data
	var packet []byte
    packet = append(packet, 1) // Secret knock (Producer)
    packet = append(packet, topicLen...)
    packet = append(packet, topicBytes...)
    packet = append(packet, dataLen...)
    packet = append(packet, data...)

    // send in one single TCP write
    _, err = p.conn.Write(packet)
    return err
}



// ======================================================================================
// Broker - Centralized server that accepts logs and organizes them
// into topics then persists them to disk
// ======================================================================================

// listens for incoming producers and consumers
func startServer() {

	// open tcp port
	ln, err := net.Listen("tcp", ":9092")
	if err != nil {
		fmt.Println("Port failed to open")
		return
	}

	fmt.Println("Broker is listening on port 9092...")
	
	// run a loop that listens for connections
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection")
			continue
		}

		// when a connection comes in, send to handleConnection()
		// in a go routine for speed and load handling
		go handleConnection(conn)
	}
}

// determines if incoming connection is producer or consumer
func handleConnection(conn net.Conn) {

	idBuffer := make([]byte, 1)

	_, err := conn.Read(idBuffer)
	if err != nil {
		fmt.Println("Failed to read id")
		return
	}

	connectionType := idBuffer[0]

	switch connectionType {
	case 1:
		acceptLog(conn)

	case 2:
		streamLogs(conn)
		return

	default:
		fmt.Println("Unknown connection type")
		conn.Close()
		return
	}

}

// coordinate storing the message safely
func acceptLog(conn net.Conn) {

	// read topic from conn
	topicLen, err := readLength(conn)
	if err != nil {
		fmt.Println("Error reading file length")
		return
	}
	// translates protobuf into the actual topic
	topicBuf := make([]byte, topicLen)
	_, err = io.ReadFull(conn, topicBuf)
	if err != nil {
		fmt.Println("Error reading topic")
		return
	}
	fileTopic := string(topicBuf)


	// read length of protobuf bytes
	dataLen, err := readLength(conn)
	if err != nil {
		fmt.Println("Error reading data length")
		return
	}
	// translates the actual protobuf into payload
	dataBuf := make([]byte, dataLen)
	_, err = io.ReadFull(conn, dataBuf)
	if err != nil {
		fmt.Println("Error reading data payload")
		return
	}

	
	// create "Logs" folder if it does not exist
	err = os.MkdirAll("Logs", 0755)
	if err != nil {
		fmt.Println("Error creating Log file")
		return
	}

	// open/create file for specific topic
	filename := fmt.Sprintf("Logs/%s.log", fileTopic)
	file, err := os.OpenFile(filename, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Error opening file")
		return
	}
	defer file.Close()

	// check current size of the file for byte position
	fileSize, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file stat")
		return
	}

	// lock to prevent weird changes
	offsetMutex.Lock()
	// if the topicOffsetMap does not exist, make one
	if topicOffsetMap[fileTopic] == nil {
		topicOffsetMap[fileTopic] = make(map[int]int64)
	}

	// current offset of topic is most recently appended topic to map
	currentOffset := topicOffsetCount[fileTopic]
	// the current offset of the current topic is where this log begins
	topicOffsetMap[fileTopic][currentOffset] = fileSize.Size()
	// increment the file topic offset
	topicOffsetCount[fileTopic]++
	// unlock to allow next thread to edit
	offsetMutex.Unlock()

	// send the file and the data to persist to it to save to disk
	persistLog(file, dataBuf)
}

// write the raw data to drive
func persistLog(file *os.File, data []byte) {

	// write the bytes to the file
	file.Write(data)
	file.Sync()
}

// streams data from disk to consumer
func streamLogs(conn net.Conn) {

	// just like in acceptLog, get the file length and name from protobuf
	// read topic from conn
	topicLen, err := readLength(conn)
	if err != nil {
		fmt.Println("Error reading file length")
		return
	}
	// translates protobuf into the actual topic
	topicBuf := make([]byte, topicLen)
	_, err = io.ReadFull(conn, topicBuf)
	if err != nil {
		fmt.Println("Error reading topic")
		return
	}
	fileTopic := string(topicBuf)


	// read the offset value from conn
	startOffset, err := readOffset(conn)
	if err != nil {
		fmt.Println("Error reading start offset")
		return
	}

	// lock to prevent weird edits
	offsetMutex.Lock()
	// make sure the topic's offset map exists
	offsetForTopic, exists := topicOffsetMap[fileTopic]
	if !exists {
		fmt.Println("Topic offset map does not exist")
		offsetMutex.Unlock()
		return
	}
	// make sure topic's requested offset index exists and produce the target byte off of that
	targetByte, exists := offsetForTopic[int(startOffset)]
	offsetMutex.Unlock()

	// return if it does not exist
	if !exists {
		return
	}

	// open folder / file for streaming
	filename := fmt.Sprintf("Logs/%s.log", fileTopic)
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file")
		return
	}
	defer file.Close()

	_, err = file.Seek(targetByte, io.SeekStart)
	if err != nil {
		fmt.Println("Error seeking to offset")
		return
	}

	// lock to prevent weird edits
	offsetMutex.Lock()
	// init variable
	var messageLength int64

	// check to see if there is a message next to the one being streamed
	nextByte, nextExists := offsetForTopic[int(startOffset)+1]
	// if so, find the length by subtracting start and current byte
	if nextExists {
		messageLength = nextByte - targetByte
	} else {
		// get total file size
		fileInfo, err := file.Stat()
		if err != nil {
			offsetMutex.Unlock()
			return
		}
		// if the log is at the end, read the rest of the file
		messageLength = fileInfo.Size() - targetByte
	}
	// unlock to proceed
	offsetMutex.Unlock()

	buf := make([]byte, messageLength)

	_, err = file.Read(buf)
	if err != nil {
		fmt.Println("Error reading buffer")
	}

	// need to send data directly to consumer over tcp connection
	conn.Write(buf)
}

// reads the length of the file when passed to acceptLog()
func readLength(conn net.Conn) (int32, error) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		return 0, err
	}

	return int32(binary.BigEndian.Uint32(buf)), nil
}

// reads the offset of the file when reading from streamLogs()
func readOffset(conn net.Conn) (int64, error) {
	buf := make([]byte, 8)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		return 0, nil
	}

	return int64(binary.BigEndian.Uint64(buf)), nil
}



// ======================================================================================
// Consumer - Applications that read logs from the broker (sequentially)
// and process them
// ======================================================================================

// filter function to handle all topics
// sends them to UI func
func (s *SystemStats) filterAndProcess(log *pb.Log) {
	s.Lock()
	defer s.Unlock()

	s.Logs++
	s.LastEvent = fmt.Sprintf("[%s] %s", log.Topic, log.Message)

	switch log.Topic {
	case "add to cart":
		s.Added++
	case "new sign up":
		s.SignUps++
	case "payment":
		s.Checkouts++
		amount, err := strconv.Atoi(log.Message)
		if err != nil {
			fmt.Println("Error parsing payment")
		}
		s.Income += amount
	}
}

// request data from a specific point in time
func processLog(address string, topic string, stats *SystemStats) {

	var currentOffset int64 = 0

	for {

		// connect to broker
		conn, err := net.Dial("tcp", address)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// tell the broker it is a consumer
		_, err = conn.Write([]byte{2})
		if err != nil {
			return
		}

		// send length of topic as well as the string
		topicBytes := []byte(topic)
		topicLenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(topicLenBuf, uint32(len(topicBytes)))

		// write length and bytes to broker
		conn.Write(topicLenBuf)
		conn.Write(topicBytes)

		// send the starting offset
		offsetBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(offsetBuf, uint64(currentOffset))
		conn.Write(offsetBuf)

		// create buffer to receive log from broker
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading buffer from broker")
			return
		}

		// unmarshal protobuf
		log := &pb.Log{}
		err = proto.Unmarshal(buf[:n], log)
		if err != nil {
			fmt.Println("Error unmarshaling log")
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// send unmarshaled log to filter
		stats.filterAndProcess(log)

		// increment offset for next topic request
		currentOffset++
	}
}

func runUI(stats *SystemStats) {
	// keep track of uptime -- just something cool to have
	upTime := time.Now()

	for {
		stats.Lock()
		logs := stats.Logs
		added := stats.Added
		checkouts := stats.Checkouts
		income := stats.Income
		signUps := stats.SignUps
		lastEvent := stats.LastEvent
		stats.Unlock()
		
		// print log to terminal
		// update terminal
		fmt.Print("\033[H\033[J")
		fmt.Println("==================================================")
        fmt.Println("       LIVE DISTRIBUTED LOG AGGREGATOR            ")
        fmt.Println("==================================================")
        fmt.Printf(" Total Logs Ingested           : %d\n", logs)
		fmt.Printf(" Total Items Added to cart     : %v\n", added)
        fmt.Printf(" Total Checkouts               : %v\n", checkouts)
		fmt.Printf(" Total Income                  : $%v\n", income)
        fmt.Printf(" Total Sign Ups                : %v\n", signUps)
        fmt.Println("==================================================")
		fmt.Printf(" Total Uptime                  : %v\n", time.Since(upTime).Round(time.Second))
		fmt.Printf(" Latest Event				   : %s\n", lastEvent)

		// sleep for just a couple seconds
		time.Sleep(50 * time.Millisecond)
	}
}

// Diagram of what it should look like
// [Producers]  ──(TCP/Protobuf)──>  [Log Broker]  ──(Appends to Disk)
//                                       │
//                                       └──(Streams)──> [Consumers]
