package messageStream

import (
	"bytes"
	ctx "context"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

// Interface for something that act as a logger, this can be either
// the logger returned by log.NewLogger or null logger which log nothing
//
type OptionalLogger interface {
	Fatal(v ...interface{})
	Fatalf(f string, v ...interface{})
	Fatalln(v ...interface{})
	Flags() int
	Output(depth int, s string) error
	Panic(v ...interface{})
	Panicf(f string, v ...interface{})
	Panicln(v ...interface{})
	Prefix() string
	Print(v ...interface{})
	Printf(f string, v ...interface{})
	Println(v ...interface{})
	SetFlags(flag int)
	SetOutput(w io.Writer)
	SetPrefix(prefix string)
	Writer() io.Writer
}

const LogEndPoint = "EndPoint"
const LogMessages = "Messages"
const LogService = "Service"

//
// EndPoint - a message stream end point is used to send/receive messages to another end point
//
type EndPoint struct {
	Connection net.Conn

	requestId int // requestId is used to associate reply packets with their corresponding requests
	mutex     sync.Mutex

	pendingRequests  map[int]chan SubmitResult
	sendChannel      chan *Element
	onClose          []func(endPoint *EndPoint)
	onPacketReceived []func(packet *Packet) bool
	loggers          *map[string]OptionalLogger

	Service      *Service
	name         string
	Id           int
	Info         interface{}
	started      bool
	UseCausality bool // Use Causality attribute in addition to _RequestID for backward compatability
}

//
// Service - wait for connection request, associate each incoming connection request with an end point
// which then processes received messages
//
type Service struct {
	Name           string
	Info           interface{}
	endPoints      map[int]*EndPoint
	listener       net.Listener
	nextEndPointId int
	createEndPoint func(service *Service, connection net.Conn) *EndPoint
	loggers        *map[string]OptionalLogger
}

type SubmitResult struct {
	Reply *Packet
	Error error
}

type NullLogger struct{}

var ErrEndPointDisconnected = fmt.Errorf("EndPoint disconnnected")
var cancelSendPacket = &Packet{EndPoint: nil, Element: nil}

const RequestIdAttributeName = "_RequestId"
const CausalityAttributeName = "Causality"

// Create a new end point and associate it with a connection
//
// name - the end point's name. When calling NewEndPoint from a service createEndPoint call back,
//        you can use an empty string to assign default name (serivce name with end point id suffix)
//
// Usually you will assign packet recieved callback
// also do not forget to start the end point if not created in the context of a service createEndPoint callback
//
// Example:
//
//	conn, err := net.Dial("tcp", "someserver.com")
//  endPoint := ms.NewEndPoint("MyConnection", conn).OnPacketReceived(func (packet *msPacket) {
//		fmt.Println("Received:", packet)
//	}).Start()
//
func NewEndPoint(name string, connection net.Conn) *EndPoint {
	endPoint := EndPoint{
		Connection:      connection,
		requestId:       0,
		pendingRequests: make(map[int]chan SubmitResult),
		sendChannel:     make(chan *Element, 10),
		name:            name,
		Id:              -1,
		started:         false,
		loggers:         nil,
	}

	endPoint.Log(LogEndPoint).Println("New endpoint: ", &endPoint)
	return &endPoint
}

// Add packet receive handler (function called when a new packet is received)
//
func (endPoint *EndPoint) OnPacketReceived(f func(packet *Packet) bool) *EndPoint {
	endPoint.onPacketReceived = append(endPoint.onPacketReceived, f)
	return endPoint
}

// Add end point close handler (function called when end point is closed)
//
// end point is either explicitly closed by calling endPoint.Close() or when
// the connection is terminated
//
func (endPoint *EndPoint) OnClose(f func(endPoint *EndPoint)) *EndPoint {
	endPoint.onClose = append(endPoint.onClose, f)
	return endPoint
}

// Add an handler for _Log messages
//
// The message has the following format
//
// _Log [Name={list of to log}] [Scope="EndPoint"|"Service"] [Disable=True|False]
//
// Default scope is Service (logging of this type will be applied to all EndPoint created by this service)
// If no specific logger Names are given, MessageStream related logs along with a supplied list of logger names will be enabled (or disabled)
//
func (endPoint *EndPoint) AddLoggingHandler(defaultLoggers []string) *EndPoint {
	return endPoint.OnPacketReceived(func(p *Packet) bool {
		if p.GetType() == "_Log" {
			disableLogging := p.Element.GetBoolAttribute("Disable", false)
			scope := p.Element.GetAttribute("Scope", "Service")
			name := p.Element.GetAttribute("Name", strings.Join(append([]string{LogEndPoint, LogMessages, LogService}, defaultLoggers...), ","))

			if disableLogging {
				if scope == "Service" && endPoint.Service != nil {
					endPoint.Service.DisableLogging(name)
				} else {
					endPoint.DisableLogging(name)
				}
			} else {
				if scope == "Service" && endPoint.Service != nil {
					endPoint.Service.EnableLogging(name)
				} else {
					endPoint.EnableLogging(name)
				}
			}

			_ = p.OptionalReply(&Attributes{"Type": "Log", "Name": strings.Join(endPoint.GetActiveLoggerNames(), ",")}).Send()

			return true
		} else {
			return false
		}
	})
}

// Start (activate the end point)
//
// The following code is typical for creating a client side end point
//
//	conn, err := net.Dial("tcp", "someserver.com")
//  endPoint := ms.NewEndPoint("MyConnection", conn).OnPacketReceived(func (packet *msPacket) {
//		fmt.Println("Received:", packet)
//	}).Start()
//
func (endPoint *EndPoint) Start() *EndPoint {
	go endPoint.receivePackets()
	go endPoint.sendPackets()
	endPoint.started = true

	endPoint.Log(LogEndPoint).Println("Started", endPoint)
	return endPoint
}

// Close the end point and disconnect it
//
func (endPoint *EndPoint) Close() error {
	var err error = nil

	endPoint.Log(LogEndPoint).Println("Closed")

	if endPoint.Connection != nil {
		close(endPoint.sendChannel)

		for _, onStreamClose := range endPoint.onClose {
			onStreamClose(endPoint)
		}

		for _, pendingReplyChannel := range endPoint.pendingRequests {
			pendingReplyChannel <- SubmitResult{nil, ErrEndPointDisconnected}
			close(pendingReplyChannel)
		}

		// Set endPoint.Connection to nil before closing the connection, because calling the connection
		// will call Close again
		//
		conn := endPoint.Connection
		endPoint.Connection = nil

		err = conn.Close()

		endPoint.onPacketReceived = nil
		endPoint.onClose = nil
	}

	return err
}

// Is EndPoint connected
func (endPoint *EndPoint) IsConnected() error {
	if endPoint.Connection == nil {
		return fmt.Errorf("EndPoint %v is not connected", endPoint)
	}
	return nil
}

// Is EndPoint started
func (endPoint *EndPoint) IsStarted() bool {
	return endPoint.started
}

// Get the Endpoint's name
func (endPoint *EndPoint) Name() string {
	return endPoint.name
}

// Set the EndPoint name
func (endPoint *EndPoint) SetName(name string) *EndPoint {
	endPoint.Log(LogEndPoint).Println("Set name to of ", endPoint, " to ", name)
	endPoint.name = name
	return endPoint
}

// Get a logger with a specific name.
//
// If the logger is enabled, a logger that does something is returned
// If the logger is not enabled, a logger which does nothing is returned
//
// A typical usage:
//
//   endPoint.Log("LogTemperatures").Println("It is hot today")
//
// A logger can be enabled either in end point scope (only for this specific endpoint) or
// in a service scope (for all endpoint created by the service)
//
func (endPoint *EndPoint) Log(name string) OptionalLogger {
	logger := endPoint.getLogger(name)
	if logger != nil {
		return logger
	}

	return NullLogger{}
}

// Enable a logger with a given name. If logger is enabled then things of this type will be logged
//
// For example:
//   endPoint.EnableLogger("Messages") will cause all messages received/sent by the end point to be logged
//
func (endPoint *EndPoint) EnableLogging(names string) *EndPoint {
	if len(names) > 0 {
		if endPoint.loggers == nil {
			loggers := make(map[string]OptionalLogger)
			endPoint.loggers = &loggers
		}

		namesList := strings.Split(names, ",")

		for _, name := range namesList {
			if _, found := (*endPoint.loggers)[name]; !found {
				(*endPoint.loggers)[name] = log.New(log.Writer(), endPoint.String()+" ("+name+") ", log.Flags())
			}
		}
	}

	return endPoint
}

// Disable a logger with a given name. If logger is disable then things of this type will not be logged
//
// For example:
//   endPoint.DisableLogger("Messages") will stop logging of all messages received/sent by the end point
//
func (endPoint *EndPoint) DisableLogging(names string) {
	namesList := strings.Split(names, ",")

	for _, name := range namesList {
		if endPoint.loggers != nil {
			delete(*endPoint.loggers, name)
			if len(*endPoint.loggers) == 0 {
				endPoint.loggers = nil
			}
		}
	}
}

// Get a list of all the loggers which are enabled for this end point (this does not include loggers which are
// enabled for the service)
//
func (endPoint *EndPoint) GetLoggerNames() []string {
	if endPoint.loggers != nil {
		result := make([]string, 0, len(*endPoint.loggers))

		for name := range *endPoint.loggers {
			result = append(result, name)
		}

		return result
	}

	return []string{}
}

// Get a list of all the logger which are enabled, both loggers which are enabled just for this end point and those
// enabled for the service
//
func (endPoint *EndPoint) GetActiveLoggerNames() []string {
	endPointLoggers := endPoint.GetLoggerNames()
	serviceLoggers := []string{}

	if endPoint.Service != nil {
		serviceLoggers = endPoint.Service.GetLoggerNames()
	}

	return append(serviceLoggers, endPointLoggers...)
}

// Look for a looger with a specific name
// return nil if logger with this name cannot be found
//
func (endPoint *EndPoint) getLogger(name string) OptionalLogger {
	if endPoint.loggers != nil {
		if logger, found := (*endPoint.loggers)[name]; found {
			return logger
		}
	}

	if endPoint.Service != nil {
		return endPoint.Service.getLogger(name)
	}

	return nil
}

//
//  Create a new service (server side):
//
//  name - service name
//  metwork, address - where sevice will listen
//  createEndPoint - function that associats end point with accepted connection
//
//  Example:
//
//  myService := ms.NewService("My service", "tcp", ":1000", func (service *ms.Service, conn ney.Conn) *ms.EndPoint) {
//		return ms.NewEndPoint("", conn).OnPacketReceived(func (packet *ms.Packet) {
//			// Do something with the packet...
//		})
//	})
//
func NewService(name string, network string, address string, info interface{}, createEndPoint func(service *Service, conn net.Conn) *EndPoint) *Service {
	service := Service{
		Name:           name,
		endPoints:      make(map[int]*EndPoint),
		nextEndPointId: 0,
		createEndPoint: createEndPoint,
		Info:           info,
		loggers:        nil,
	}

	go service.acceptStreams(network, address)

	return &service
}

//
// Terminate a service
//
func (service *Service) Terminate() {
	if service.listener != nil {
		service.listener.Close()
	}

	// Closing connection should remove the entry from the map, therefore first
	// create an array with all the endpoints to avoid the map changing while iteratating
	//
	var endPoints []*EndPoint

	for _, endPoint := range service.endPoints {
		endPoints = append(endPoints, endPoint)
	}

	for _, endPoint := range endPoints {
		endPoint.Close()
	}
}

//
// Logger functions
//

// Get a logger with a specific name.
//
// If the logger is enabled, a logger that does something is returned
// If the logger is not enabled, a logger which does nothing is returned
//
// A typical usage:
//
//   service.Log("LogTemperatures").Println("It is hot today")
//
func (service *Service) Log(name string) OptionalLogger {
	if logger := service.getLogger(name); logger != nil {
		return logger
	}

	return NullLogger{}
}

// Enable a logger with a given name. If logger is enabled then things of this type will be logged
//
// For example:
//   service.EnableLogger("Service") will cause all messages assocaited with service state to be logged
//
func (service *Service) EnableLogging(names string) *Service {
	if len(names) > 0 {
		if service.loggers == nil {
			loggers := make(map[string]OptionalLogger)
			service.loggers = &loggers
		}

		namesList := strings.Split(names, ",")

		for _, name := range namesList {
			if _, found := (*service.loggers)[name]; !found {
				(*service.loggers)[name] = log.New(log.Writer(), service.Name+" ("+name+") ", log.Flags())
			}
		}
	}

	return service
}

// Disable a logger with a given name. If logger is disable then things of this type will not be logged
//
// For example:
//   servicet.DisableLogger("Service") will stop logging of service state related messages
//
func (service *Service) DisableLogging(names string) {
	namesList := strings.Split(names, ",")

	for _, name := range namesList {
		if service.loggers != nil {
			delete(*service.loggers, name)
			if len(*service.loggers) == 0 {
				service.loggers = nil
			}
		}
	}
}

// Get a list of all the loggers which are enabled for this service
//
func (service *Service) GetLoggerNames() []string {
	if service.loggers != nil {
		result := make([]string, 0, len(*service.loggers))

		for name := range *service.loggers {
			result = append(result, name)
		}
		return result
	}

	return []string{}
}

// Look for a looger with a specific name
// return nil if logger with this name cannot be found
//
func (service *Service) getLogger(name string) OptionalLogger {
	if service.loggers == nil {
		return nil
	}

	if logger, found := (*service.loggers)[name]; found {
		return logger
	}

	return nil
}

//
// Message creation functions
//

//
// Create message that is suitable for broadcasting
//
//  service.Bradcast(Message(Attributes{"Type": "SundayArrived"}))
//
func (service *Service) Message(content ...interface{}) *BroadcastPacket {
	return &BroadcastPacket{
		NewElement("Message", content),
		service,
	}
}

//
//  Create a message that can be sent
//
//  endPoint.Message(Attributes{"Type" : "SundayArrived"}).Send()
//
func (endPoint *EndPoint) Message(content ...interface{}) *Packet {
	return endPoint.newPacket("Message", content)
}

//
//  Create a request packet that can submitted
//
//   reply := <- endPoint.Request(Attributes{"Type" : "FindMyWife"}).Submit()
//
func (endPoint *EndPoint) Request(content ...interface{}) *Packet {
	return endPoint.newPacket("Request", content)
}

//
// Return requestID (if one exists)
//
func (requestPacket *Packet) GetRequestId() (int, error) {
	if id := requestPacket.Element.GetIntAttribute(RequestIdAttributeName, -1); id >= 0 {
		return id, nil
	} else if id := requestPacket.Element.GetIntAttribute(CausalityAttributeName, -1); id >= 0 {
		return id, nil
	} else {
		return -1, fmt.Errorf("Packet: %v  has no request ID - cannot reply", requestPacket)
	}
}

func (packet *Packet) IsRequest() bool {
	id, err := packet.GetRequestId()

	if id >= 0 && err == nil {
		return true
	} else {
		return false
	}
}

//
// Get message/request type
//
func (packet *Packet) GetType() string {
	return packet.Element.GetAttribute("Type", "*Missing*")
}

//
// Create a reply to a request
//
//  didSundayArrivedRequest.Reply(Attributes{"Answer" : true}).Send()
//
func (requestPacket *Packet) Reply(content ...interface{}) *Packet {
	if requestPacket.EndPoint != nil {
		if requestId, err := requestPacket.GetRequestId(); err == nil {
			content = append(content, Attributes{RequestIdAttributeName: requestId})
			if requestPacket.EndPoint.UseCausality {
				content = append(content, Attributes{CausalityAttributeName: requestId})
			}

			return requestPacket.EndPoint.newPacket("Reply", content)
		} else {
			log.Fatalln(err.Error())
			return nil
		}
	} else {
		log.Fatalln("Trying to reply packet with closed connection: ", requestPacket)
		return nil
	}
}

//
// OptionalReply - create reply packet if the sending packet is request. Otherwise, return null
//
func (maybeRequestPacket *Packet) OptionalReply(content ...interface{}) *Packet {
	if maybeRequestPacket.IsRequest() {
		return maybeRequestPacket.Reply(content)
	} else {
		return cancelSendPacket
	}
}

//
// Create exception packet
//
func (requestPacket *Packet) Exception(exception error, content ...interface{}) *Packet {
	if requestPacket.EndPoint != nil {
		content = append(content, Attributes{"Description": exception.Error()})
		if requestId, err := requestPacket.GetRequestId(); err == nil {
			content = append(content, Attributes{RequestIdAttributeName: requestId})
			if requestPacket.EndPoint.UseCausality {
				content = append(content, Attributes{CausalityAttributeName: requestId})
			}
		}

		return requestPacket.EndPoint.newPacket("Exception", content)
	} else {
		log.Fatalln("Trying to generate exception for packet with closed connection: ", requestPacket)
		return nil
	}
}

//
// Actions
//
//  Send either a message or reply packet.
//
//  Sending a message:
//   endPoint.Message(Attributes{"Type", "SundayArrived"}).Send()
//
//  Reply to a request
//   pingRequest.Reply(Attributes{"Type", "PingResult"}).Send()
//
func (packet *Packet) Send() error {
	if packet == cancelSendPacket {
		return nil
	}

	if packet.EndPoint != nil && packet.Element.Name != "Request" {
		return packet.EndPoint.sendPacket(packet.Element)
	} else {
		log.Fatalln("Trying to send packet with the wrong type: ", packet)
		return nil
	}
}

//
//  Submit request and get a channel on which reply is received
//
//   For example: pingResult := <- endPoint.Request(Attributes{"Type" : "Ping"}).Submit()
//
func (packet *Packet) Submit(ctx ctx.Context) chan SubmitResult {
	if packet.EndPoint == nil || packet.Element.Name != "Request" {
		log.Fatalln("Trying to submit invalid packet (only Request packets can be submitted", packet)
	}

	endPoint := packet.EndPoint

	replyChannel := make(chan SubmitResult)
	requestReplyChannel := make(chan SubmitResult)

	endPoint.mutex.Lock()

	endPoint.requestId += 1
	requestId := endPoint.requestId

	packet.Element.Attributes[RequestIdAttributeName] = requestId
	if packet.EndPoint.UseCausality {
		packet.Element.Attributes[CausalityAttributeName] = requestId
	}

	endPoint.pendingRequests[requestId] = requestReplyChannel

	endPoint.mutex.Unlock()

	err := endPoint.sendPacket(packet.Element)
	if err != nil {
		log.Println("Error submitting packet:", packet)
		endPoint.removePendingRequest(requestId)

		go func() {
			replyChannel <- SubmitResult{nil, err}
			close(replyChannel)
		}()
	} else {
		go func() {
			select {
			case <-ctx.Done():
				endPoint.removePendingRequest(requestId)
				replyChannel <- SubmitResult{nil, ctx.Err()}

			case r := <-requestReplyChannel:

				replyChannel <- SubmitResult{r.Reply, r.Error}
			}

			close(replyChannel)
		}()
	}

	return replyChannel
}

//
//  Broadcast a packet to all streams connected to a service
//
func (service *Service) Broadcast(packet *BroadcastPacket, sendPredicate func(endPoint *EndPoint, packet *BroadcastPacket) bool) error {
	for _, endPoint := range service.endPoints {
		if sendPredicate == nil || sendPredicate(endPoint, packet) {
			err := endPoint.sendPacket(packet.Element)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (broadcastPacket *BroadcastPacket) Broadcast(sendPredicate func(endPoint *EndPoint, packet *BroadcastPacket) bool) error {
	return broadcastPacket.Service.Broadcast(broadcastPacket, sendPredicate)
}

//
//  Non exported functions
//

func (endPoint *EndPoint) newPacket(packetType string, content ...interface{}) *Packet {
	return &Packet{
		NewElement(packetType, content),
		endPoint,
	}
}

func (endPoint *EndPoint) sendPacket(element *Element) error {
	if err := endPoint.IsConnected(); err != nil {
		return err
	}

	if !endPoint.IsStarted() {
		panic(fmt.Sprintf("Endpoint %v is not started - did you call endPoint.Start()?", endPoint))
	}

	endPoint.sendChannel <- element
	return nil
}

func (endPoint *EndPoint) removePendingRequest(requestId int) {
	endPoint.lock("delete pending request")
	delete(endPoint.pendingRequests, requestId)
	endPoint.unlock("delete pedning request")
}

func (endPoint *EndPoint) receivePackets() {
	for {
		packetBytes, err := endPoint.readPacketBytes()

		if err == io.EOF || len(packetBytes) == 0 {
			break
		}

		var packet Packet = Packet{EndPoint: endPoint}

		packet.Element, err = decode(packetBytes)

		if err != nil {
			log.Fatalln(fmt.Sprintf("MessageStream %s error while decoding incoming packet %v", endPoint.name, err))
			fmt.Println("packetBytes length", len(packetBytes))
			fmt.Println("packetBytes: ", packetBytes)
		}

		endPoint.Log(LogMessages).Println("Received: ", packet)

		if packet.Element.Name == "Reply" || packet.Element.Name == "Exception" {
			requestId := packet.Element.GetIntAttribute(RequestIdAttributeName, -1)

			if requestId >= 0 {
				pendingReplyChannel, foundPendingRequest := endPoint.pendingRequests[requestId]

				if foundPendingRequest {
					endPoint.removePendingRequest(requestId)

					if packet.Element.Name == "Reply" {
						pendingReplyChannel <- SubmitResult{&packet, nil}
					} else {
						pendingReplyChannel <- SubmitResult{&packet, fmt.Errorf("Exception: %v", packet.Element.GetAttribute("Description", "Exception has no description"))}
					}

					close(pendingReplyChannel)
				} else {
					log.Println("Received reply packet with no pending request: ", packet)
				}
			}
		} else {
			if endPoint.onPacketReceived != nil {
				for _, onPacketReceived := range endPoint.onPacketReceived {
					if onPacketReceived(&packet) {
						break // If returned true, packet was handled - no need to call next inline
					}
				}
			}
		}
	}

	endPoint.Close()
}

func (endPoint *EndPoint) sendPackets() {
	for element := range endPoint.sendChannel {
		endPoint.Log(LogMessages).Println("Sending: ", element)
		packetBytes, err := element.encode()

		if err != nil {
			panic(err)
		}

		err = endPoint.sendPacketBytes(packetBytes)
		if err != nil {
			endPoint.Log(LogEndPoint).Println("Packet write error: ", err, " - closing endPoint")
			endPoint.Close()
			break
		}
	}
}

func (service *Service) acceptStreams(network string, address string) {
	var err error

	service.listener, err = net.Listen(network, address)

	if err != nil {
		panic(err)
	}

	for {
		connection, err := service.listener.Accept()

		if err != nil {
			break
		}

		endPoint := service.createEndPoint(service, connection).OnClose(func(endPoint *EndPoint) {
			delete(service.endPoints, endPoint.Id)
		})

		endPoint.Service = service

		endPoint.Id = service.nextEndPointId
		if endPoint.name == "" {
			endPoint.name = service.Name + " " + strconv.Itoa(service.nextEndPointId)
		}

		service.endPoints[endPoint.Id] = endPoint
		service.nextEndPointId += 1

		endPoint.Start()
	}

	service.listener = nil
	service.Log(LogService).Println("Terminated")
}

func (endPoint *EndPoint) lock(m string) {
	//fmt.Println("Lock: ", m)
	endPoint.mutex.Lock()
}

func (endPoint *EndPoint) unlock(m string) {
	//fmt.Println("Unlock: ", m)
	endPoint.mutex.Unlock()
}

func (endPoint *EndPoint) sendPacketBytes(packetBytes []byte) error {
	countBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBytes, uint32(len(packetBytes)))

	_, err := endPoint.Connection.Write(countBytes)

	if err == nil {
		_, err = endPoint.Connection.Write(packetBytes)
	}

	return err
}

func (endPoint *EndPoint) readPacketBytes() ([]byte, error) {
	if endPoint.Connection != nil {
		countBytes := make([]byte, 4)
		_, err := endPoint.Connection.Read(countBytes)

		if err != nil {
			return nil, io.EOF
		}

		count := binary.LittleEndian.Uint32(countBytes)
		packetBytes := make([]byte, count)

		totalRead := 0
		for totalRead < int(count) {
			n, err := endPoint.Connection.Read(packetBytes[totalRead:])

			if err != nil {
				return packetBytes, err
			}

			totalRead += n
		}

		return packetBytes, err
	} else {
		return nil, io.EOF
	}
}

func (endPoint *EndPoint) String() string {
	s := endPoint.name
	info, hasInfo := endPoint.Info.(fmt.Stringer)

	if hasInfo {
		s += ", info: " + info.String()
	}

	return s
}

func (element *Element) encode() ([]byte, error) {
	buffer := bytes.NewBuffer(nil)
	xmlEncoder := xml.NewEncoder(buffer)

	err := element.encodeElement(xmlEncoder)
	xmlEncoder.Flush()

	return buffer.Bytes(), err
}

func (element *Element) encodeElement(xmlEncoder *xml.Encoder) error {
	var attr []xml.Attr
	var err error

	for name, value := range element.Attributes {
		attr = append(attr, xml.Attr{Name: xml.Name{Space: "", Local: name}, Value: toString(value)})
	}

	xmlElement := xml.StartElement{Name: xml.Name{Space: "", Local: element.Name}, Attr: attr}

	err = xmlEncoder.EncodeToken(xmlElement)
	if err != nil {
		return err
	}

	for childIndex, child := range element.Children {
		switch childElement := child.(type) {

		case Element:
			err = childElement.encodeElement(xmlEncoder)
		case *Element:
			err = childElement.encodeElement(xmlEncoder)
		case string:
			err = xmlEncoder.EncodeToken(xml.CharData(childElement))
		case int:
			err = xmlEncoder.EncodeToken(xml.CharData(strconv.Itoa(childElement)))
		case bool:
			s := "false"

			if childElement {
				s = "true"
			}
			err = xmlEncoder.EncodeToken((xml.CharData(s)))

		default:
			panic(fmt.Sprintf("Element child %d has invalid type %T (value is %v)", childIndex, child, child))
		}

		if err != nil {
			return err
		}
	}

	err = xmlEncoder.EncodeToken(xmlElement.End())

	return err
}

func decode(packetPacket []byte) (*Element, error) {
	decoder := xml.NewDecoder(bytes.NewBuffer(packetPacket))

	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}

	return decodeToken(decoder, token.(xml.StartElement))
}

func decodeToken(decoder *xml.Decoder, startElement xml.StartElement) (*Element, error) {
	element := Element{
		Name:       startElement.Name.Local,
		Attributes: make(Attributes),
	}

	for _, attr := range startElement.Attr {
		element.Attributes[attr.Name.Local] = attr.Value
	}

	done := false

	for !done {
		aToken, err := decoder.Token()

		if err != nil {
			return nil, err
		}

		switch token := aToken.(type) {
		case xml.EndElement:
			done = true

		case xml.StartElement:
			childElement, err := decodeToken(decoder, token)
			if err != nil {
				return nil, err
			}

			element.Children = append(element.Children, childElement)

		case xml.CharData:
			element.Children = append(element.Children, string(token))
		}
	}

	return &element, nil
}

func (l NullLogger) Fatal(v ...interface{})            {}
func (l NullLogger) Fatalf(f string, v ...interface{}) {}
func (l NullLogger) Fatalln(v ...interface{})          {}
func (l NullLogger) Flags() int                        { return 0 }
func (l NullLogger) Output(depth int, s string) error  { return nil }
func (l NullLogger) Panic(v ...interface{})            {}
func (l NullLogger) Panicf(f string, v ...interface{}) {}
func (l NullLogger) Panicln(v ...interface{})          {}
func (l NullLogger) Prefix() string                    { return "" }
func (l NullLogger) Print(v ...interface{})            {}
func (l NullLogger) Printf(f string, v ...interface{}) {}
func (l NullLogger) Println(v ...interface{})          {}
func (l NullLogger) SetFlags(flag int)                 {}
func (l NullLogger) SetOutput(w io.Writer)             {}
func (l NullLogger) SetPrefix(prefix string)           {}
func (l NullLogger) Writer() io.Writer                 { return nil }
