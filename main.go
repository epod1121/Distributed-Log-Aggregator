package main

import (
	"encoding/binary"
	"encoding/json"
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
	consumerOffsets = make(map[string]map[string]int64)
	consumerMutex sync.Mutex
	offsetMutex sync.Mutex
)

type IndexData struct {
	Map map[string]map[int]int64 `json:"map"`
	Count map[string]int `json:"count"`
}

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
	go processLog("localhost:9092","analytics-ui", "payment", stats)
	go processLog("localhost:9092","analytics-ui", "add to cart", stats)
	go processLog("localhost:9092","analytics-ui", "new sign up", stats)

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

	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		return err
	}
	defer conn.Close()

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
    _, err = conn.Write(packet)
    return err
}



// ======================================================================================
// Broker - Centralized server that accepts logs and organizes them
// into topics then persists them to disk
// ======================================================================================

// on startup to find where the last program instance left off
func saveConsumerOffsetDisk() {
	data, err := json.Marshal(consumerOffsets)
	if err != nil {
		return
	}

	_ = os.WriteFile("Logs/__consumer_offsets.json", data, 0644)
}

func loadConsumerOffsetDisk() {
	data, err := os.ReadFile("Logs/__consumer_offsets.json")
	if err != nil {
		fmt.Println("Files does not exist")
		return
	}
	_ = json.Unmarshal(data, &consumerOffsets)
}

func saveTopicIndexDisk() {
	data, err := json.Marshal(IndexData{
		Map: topicOffsetMap,
		Count: topicOffsetCount,
	})
	if err != nil {
		return
	}

	_ = os.WriteFile("Logs/__topic_index.json", data, 0644)
}

func loadTopicIndexDisk() {
	data, err := os.ReadFile("Logs/__topic_index.json")
	if err != nil {
		fmt.Println("File does not exist")
		return
	}

	var idx IndexData
	if err := json.Unmarshal(data, &idx); err == nil {
		if idx.Map != nil {
			topicOffsetMap = idx.Map
		}
		if idx.Count != nil {
			topicOffsetCount = idx.Count
		}
	}
}

// listens for incoming producers and consumers
func startServer() {
	
	// make folder
	_ = os.MkdirAll("Logs", 0755)

	loadTopicIndexDisk()
	loadConsumerOffsetDisk()

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

	// so no connection path leaks
	defer conn.Close()

	idBuffer := make([]byte, 1)

	_, err := conn.Read(idBuffer)
	if err != nil {
		fmt.Println("Failed to read id")
		conn.Close()
		return
	}

	connectionType := idBuffer[0]

	switch connectionType {
	case 1:
		acceptLog(conn)
		conn.Close()

	case 2:
		streamLogs(conn)

	case 3:
		handleFunctionOffset(conn)

	case 4:
		handleCommitOffset(conn)

	default:
		fmt.Println("Unknown connection type")
		conn.Close()
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

	// send the file and the data to persist to it to save to disk
	persistLog(file, dataBuf)

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
	
	saveTopicIndexDisk()

	// unlock to allow next thread to edit
	offsetMutex.Unlock()
}

// write the raw data to drive
func persistLog(file *os.File, data []byte) {

	// write the bytes to the file
	file.Write(data)
	file.Sync()
}

// streams data from disk to consumer
func streamLogs(conn net.Conn) {

	// close the connection if offset doesn't exist
	defer conn.Close()

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
	// return if it does not exist
	if !exists {
		offsetMutex.Unlock()
		return
	}

	// init variable
	var messageLength int64
	// check to see if there is a message next to the one being streamed
	nextByte, nextExists := offsetForTopic[int(startOffset)+1]
	// if so, find the length by subtracting start and current byte
	if nextExists {
		messageLength = nextByte - targetByte
	} else {
		filename := fmt.Sprintf("Logs/%s.log", fileTopic)
		fileInfo, err := os.Stat(filename)
		if err != nil {
			offsetMutex.Unlock()
			return
		}
		messageLength = fileInfo.Size() - targetByte
	}
	offsetMutex.Unlock()

	if messageLength <= 0 {
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
		return 0, err
	}

	return int64(binary.BigEndian.Uint64(buf)), nil
}

// find out where offset was last left off
func handleFunctionOffset(conn net.Conn){

	// read group ID
	groupIDLen, err := readLength(conn)
	if err != nil {
		fmt.Println("Could not read group ID")
		return
	}

	groupIDBuf := make([]byte, groupIDLen)
	
	if _, err := io.ReadFull(conn, groupIDBuf); err != nil {
		return
	}
	groupID := string(groupIDBuf)

	// read topic
	topicLen, err := readLength(conn)
	if err != nil {
		return
	}

	topicBuf := make([]byte, topicLen)
	if _, err := io.ReadFull(conn, topicBuf); err != nil {
		return
	}
	topic := string(topicBuf)

	// get offset from map
	offset := getCommittedOffset(groupID, topic)

	// respond with 8 byte int64
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(offset))
	conn.Write(buf)
}

func handleCommitOffset(conn net.Conn){
	
	// read group ID
	groupIDLen, err := readLength(conn)
	if err != nil {
		fmt.Println("Could not read group ID")
		return
	}

	groupIDBuf := make([]byte, groupIDLen)
	
	if _, err := io.ReadFull(conn, groupIDBuf); err != nil {
		return
	}
	groupID := string(groupIDBuf)

	// read topic
	topicLen, err := readLength(conn)
	if err != nil {
		return
	}

	topicBuf := make([]byte, topicLen)
	if _, err := io.ReadFull(conn, topicBuf); err != nil {
		return
	}
	topic := string(topicBuf)

	// read offset
	committedOffset, err := readOffset(conn)
	if err != nil {
		return
	}

	// update memory
	commitOffset(groupID, topic, committedOffset)

	// send ACK back to consumer
	conn.Write([]byte{1})
}

func getCommittedOffset(groupID string, topic string) int64 {

	consumerMutex.Lock()
	defer consumerMutex.Unlock()

	if consumerOffsets[groupID] == nil {
		// default to 0 if never committed before
		return 0
	}
	return consumerOffsets[groupID][topic]
}

func commitOffset(groupID string, topic string, offset int64) {
	consumerMutex.Lock()
	defer consumerMutex.Unlock()

	if consumerOffsets[groupID] == nil {
		consumerOffsets[groupID] = make(map[string]int64)
	}
	consumerOffsets[groupID][topic] = offset

	saveConsumerOffsetDisk()
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
func processLog(address string, groupID string, topic string, stats *SystemStats) {

	// fund out where everything was last left off
	currentOffset := fetchOffsetFromServer(address, groupID, topic)

	fmt.Printf("[%s] Resuming %s from offset %d\n", groupID, topic, currentOffset)

	for {

		// connect to broker
		conn, err := net.Dial("tcp", address)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// tell the broker it is a consumer
		if _, err = conn.Write([]byte{2}); err != nil {
			conn.Close()
			time.Sleep(50 * time.Millisecond)
			continue
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
		conn.Close()

		if err != nil || n == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
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
		_ = commitOffsetToServer(address, groupID, topic, currentOffset)
	}
}

func fetchOffsetFromServer(address string, groupID string, topic string) int64 {

	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("Could not dial address")
		return 0
	}
	defer conn.Close()

	// send knock
	conn.Write([]byte{3})

	// send group ID, length, and string
	groupBytes := []byte(groupID)
	groupLenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(groupLenBuf, uint32(len(groupBytes)))
	conn.Write(groupLenBuf)
	conn.Write(groupBytes)

	// send topic, length, and string
	topicBytes := []byte(topic)
	topicLenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(topicLenBuf, uint32(len(topicBytes)))
	conn.Write(topicLenBuf)
	conn.Write(topicBytes)

	// read back 8 byte offset
	buf := make([]byte, 8)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return 0
	}

	return int64(binary.BigEndian.Uint64(buf))
}

func commitOffsetToServer(address string, groupID string, topic string, offset int64) error {

	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("Could not dial address")
		return err
	}
	defer conn.Close()

	// send knock
	conn.Write([]byte{4})

	// send group ID, length, and string
	groupBytes := []byte(groupID)
	groupLenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(groupLenBuf, uint32(len(groupBytes)))
	conn.Write(groupLenBuf)
	conn.Write(groupBytes)

	// send topic, length, and string
	topicBytes := []byte(topic)
	topicLenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(topicLenBuf, uint32(len(topicBytes)))
	conn.Write(topicLenBuf)
	conn.Write(topicBytes)

	// send offset to commit
	offsetBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBuf, uint64(offset))
	conn.Write(offsetBuf)

	// read 1 byte ACK
	ack := make([]byte, 1)
	_, err = conn.Read(ack)
	return err
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
		fmt.Printf(" Latest Event		       : %s\n", lastEvent)

		// sleep for just a couple seconds
		time.Sleep(50 * time.Millisecond)
	}
}