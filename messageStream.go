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
	"sync"
)

type EndPoint struct {
	connection net.Conn

	requestId int // requestId is used to associate reply packets with their corresponding requests
	mutex     sync.Mutex

	pendingRequests  map[int]chan SubmitResult
	sendChannel      chan *Element
	onClose          []func(endPoint *EndPoint)
	onPacketReceived []func(packet *Packet)

	Service      *Service
	Name         string
	Id           int
	Info         interface{}
	started      bool
	UseCausality bool // Use Causality attribute in addition to _RequestID for backward compatability
}

type Service struct {
	Name           string
	Info           interface{}
	endPoints      map[int]*EndPoint
	listener       net.Listener
	nextEndPointId int
	createEndPoint func(service *Service, connection net.Conn) *EndPoint
}

type SubmitResult struct {
	Reply *Packet
	Error error
}

var ErrEndPointDisconnected = fmt.Errorf("EndPoint disconnnected")

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
		connection:      connection,
		requestId:       0,
		pendingRequests: make(map[int]chan SubmitResult),
		sendChannel:     make(chan *Element),
		Name:            name,
		Id:              -1,
		started:         false,
	}

	return &endPoint
}

// Add packet receive handler (function called when a new packet is received)
//
func (endPoint *EndPoint) OnPacketReceived(f func(packet *Packet)) *EndPoint {
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

	return endPoint
}

// Close the end point and disconnect it
//
func (endPoint *EndPoint) Close() error {
	var err error = nil

	if endPoint.connection != nil {
		close(endPoint.sendChannel)

		for _, onStreamClose := range endPoint.onClose {
			onStreamClose(endPoint)
		}

		for _, pendingReplyChannel := range endPoint.pendingRequests {
			pendingReplyChannel <- SubmitResult{nil, ErrEndPointDisconnected}
			close(pendingReplyChannel)
		}

		// Set endPoint.connection to nil before closing the connection, because calling the connection
		// will call Close again
		//
		conn := endPoint.connection
		endPoint.connection = nil

		err = conn.Close()

		endPoint.onPacketReceived = nil
		endPoint.onClose = nil

		fmt.Printf("MessageStream %v closed\n", endPoint.Name)
	}

	return err
}

func (endPoint *EndPoint) IsConnected() error {
	if endPoint.connection == nil {
		return fmt.Errorf("EndPoint %v is not connected", endPoint)
	}
	return nil
}

func (endPoint *EndPoint) IsStarted() bool {
	return endPoint.started
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
func NewService(name string, network string, address string, createEndPoint func(service *Service, conn net.Conn) *EndPoint) *Service {
	service := Service{
		Name:           name,
		endPoints:      make(map[int]*EndPoint),
		nextEndPointId: 0,
		createEndPoint: createEndPoint,
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
	endPoints := make([]*EndPoint, len(service.endPoints))

	for _, endPoint := range service.endPoints {
		endPoints = append(endPoints, endPoint)
	}

	for _, endPoint := range endPoints {
		endPoint.Close()
	}
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

func (requestPacket *Packet) GetRequestId() (int, error) {
	if id := requestPacket.Element.GetIntAttribute(RequestIdAttributeName, -1); id >= 0 {
		return id, nil
	} else if id := requestPacket.Element.GetIntAttribute(CausalityAttributeName, -1); id >= 0 {
		return id, nil
	} else {
		return -1, fmt.Errorf("Packet: %v  has no request ID - cannot reply", requestPacket)
	}
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
			log.Fatalln(fmt.Sprintf("MessageStream %s error while decoding incoming packet %v", endPoint.Name, err))
		}

		if packet.Element.Name == "Reply" {
			requestId := packet.Element.GetIntAttribute(RequestIdAttributeName, -1)

			if requestId >= 0 {
				pendingReplyChannel, foundPendingRequest := endPoint.pendingRequests[requestId]

				if foundPendingRequest {
					endPoint.removePendingRequest(requestId)

					pendingReplyChannel <- SubmitResult{&packet, nil}
					close(pendingReplyChannel)
				} else {
					log.Println("Received reply packet with no pending request: ", packet)
				}
			}
		} else {
			if endPoint.onPacketReceived != nil {
				for _, onPacketReceived := range endPoint.onPacketReceived {
					onPacketReceived(&packet)
				}
			}
		}
	}

	endPoint.Close()
}

func (endPoint *EndPoint) sendPackets() {
	for element := range endPoint.sendChannel {
		packetBytes, err := element.encode()

		if err != nil {
			panic(err)
		}

		err = endPoint.sendPacketBytes(packetBytes)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("MessageStream", endPoint, " sender terminated")
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

		endPoint.Id = service.nextEndPointId
		if endPoint.Name == "" {
			endPoint.Name = service.Name + " " + strconv.Itoa(service.nextEndPointId)
		}

		service.endPoints[endPoint.Id] = endPoint
		service.nextEndPointId += 1

		endPoint.Start()
	}

	service.listener = nil
	fmt.Printf("MessageStream Service %v terminated\n", service.Name)
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

	_, err := endPoint.connection.Write(countBytes)

	if err == nil {
		_, err = endPoint.connection.Write(packetBytes)
	}

	return err
}

func (endPoint *EndPoint) readPacketBytes() ([]byte, error) {
	if endPoint.connection != nil {
		countBytes := make([]byte, 4)
		_, err := endPoint.connection.Read(countBytes)

		if err != nil {
			return nil, io.EOF
		}

		count := binary.LittleEndian.Uint32(countBytes)
		packetBytes := make([]byte, count)

		_, err = endPoint.connection.Read(packetBytes)

		return packetBytes, err
	} else {
		return nil, io.EOF
	}
}

func (endPoint *EndPoint) String() string {
	s := endPoint.Name
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
