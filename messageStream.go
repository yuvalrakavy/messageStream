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

	requestId int // Causality is used to associate reply packets with their corresponding requests
	mutex     sync.Mutex

	pendingRequests  map[int]chan SubmitResult
	onStreamClosed   func()
	onPacketReceived func(packet *Packet)

	Service *Service
	Name    string
	Id      int
	Info    interface{}
}

type Service struct {
	Name             string
	endPoints        map[int]*EndPoint
	listener         net.Listener
	nextEndPointId   int
	onNewEndPoint    func(endPoint *EndPoint)
	onPacketReceived func(packet *Packet)
}

type SubmitResult struct {
	Reply *Packet
	Error error
}

var ErrEndPointDisconnected = fmt.Errorf("EndPoint disconnnected")

const RequestIdAttributeName = "_RequestId"

func NewEndPoint(name string, connection net.Conn, onPacketReceived func(packet *Packet)) *EndPoint {
	endPoint := EndPoint{
		connection:       connection,
		requestId:        0,
		pendingRequests:  make(map[int]chan SubmitResult),
		Name:             name,
		onPacketReceived: onPacketReceived,
		Id:               -1,
	}

	go endPoint.processInput()

	return &endPoint
}

func NewService(name string, network string, address string, onNewEndPoint func(endPoint *EndPoint), onPacketReceived func(packet *Packet)) *Service {
	service := Service{
		Name:             name,
		endPoints:        make(map[int]*EndPoint),
		nextEndPointId:   0,
		onNewEndPoint:    onNewEndPoint,
		onPacketReceived: onPacketReceived,
	}

	go service.acceptStreams(network, address)

	return &service
}

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
		endPoint.connection.Close()
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

//
// Create a reply to a request
//
//  didSundayArrivedRequest.Reply(Attributes{"Answer" : true}).Send()
//
func (requestPacket *Packet) Reply(content ...interface{}) *Packet {
	if requestPacket.EndPoint != nil && requestPacket.Name == "Request" {
		content = append(content, Attributes{RequestIdAttributeName: requestPacket.GetAttribute(RequestIdAttributeName, "-1")})
		return requestPacket.EndPoint.newPacket("Reply", content)
	} else {
		log.Fatalln("Trying to reply to wrong packet type (not a valid request packet): ", requestPacket)
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
	if packet.EndPoint != nil && packet.Name != "Request" {
		return packet.EndPoint.sendPacket(&packet.Element)
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
	if packet.EndPoint == nil || packet.Name != "Request" {
		log.Fatalln("Trying to submit invalid packet (only Request packets can be submitted", packet)
	}

	endPoint := packet.EndPoint
	replyChannel := make(chan SubmitResult)
	requestReplyChannel := make(chan SubmitResult)

	endPoint.mutex.Lock()

	endPoint.requestId += 1
	requestId := endPoint.requestId

	packet.Attributes[RequestIdAttributeName] = strconv.Itoa(requestId)
	endPoint.pendingRequests[requestId] = requestReplyChannel

	endPoint.mutex.Unlock()

	err := endPoint.sendPacket(&packet.Element)
	if err != nil {
		log.Println("Error submitting packet:", packet)
		endPoint.removePendingRequest(requestId)

		go func() {
			replyChannel <- SubmitResult{nil, err}
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
			err := endPoint.sendPacket(&packet.Element)
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
	packetBytes, err := element.encode()

	if err != nil {
		return err
	}

	return endPoint.sendPacketBytes(packetBytes)
}

func (endPoint *EndPoint) removePendingRequest(requestId int) {
	endPoint.Lock("delete pending request")
	delete(endPoint.pendingRequests, requestId)
	endPoint.Unlock("delete pedning request")
}

func (endPoint *EndPoint) processInput() {
	for {
		packetBytes, err := endPoint.readPacketBytes()

		if err == io.EOF || len(packetBytes) == 0 {
			break
		}

		var packet Packet

		err = packet.Element.decode(packetBytes)

		if err != nil {
			log.Fatalln(fmt.Sprintf("MessageStream %s error while decoding incoming packet %v", endPoint.Name, err))
		}

		packet.EndPoint = endPoint

		if packet.Name == "Reply" {
			requestId := packet.GetIntAttribute(RequestIdAttributeName, -1)

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
				endPoint.onPacketReceived(&packet)
			}
		}
	}

	if endPoint.onStreamClosed != nil {
		endPoint.onStreamClosed()
	}

	for _, pendingReplyChannel := range endPoint.pendingRequests {
		pendingReplyChannel <- SubmitResult{nil, ErrEndPointDisconnected}
		close(pendingReplyChannel)
	}

	fmt.Printf("MessageStream %v closed\n", endPoint.Name)
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

		name := service.Name + " " + strconv.Itoa(service.nextEndPointId)
		endPoint := NewEndPoint(name, connection, service.onPacketReceived)
		endPoint.Id = service.nextEndPointId
		endPoint.onStreamClosed = func() {
			delete(service.endPoints, endPoint.Id)
		}

		service.endPoints[endPoint.Id] = endPoint

		service.nextEndPointId += 1
	}

	service.listener = nil
	fmt.Printf("MessageStream Service %v terminated\n", service.Name)
}

func (endPoint *EndPoint) Lock(m string) {
	//fmt.Println("Lock: ", m)
	endPoint.mutex.Lock()
}

func (endPoint *EndPoint) Unlock(m string) {
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
	countBytes := make([]byte, 4)
	_, err := endPoint.connection.Read(countBytes)

	if err == io.EOF {
		return nil, err
	}

	count := binary.LittleEndian.Uint32(countBytes)
	packetBytes := make([]byte, count)

	_, err = endPoint.connection.Read(packetBytes)

	return packetBytes, err
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
		attr = append(attr, xml.Attr{Name: xml.Name{Space: "", Local: name}, Value: value})
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

func (element *Element) decode(packetPacket []byte) error {
	decoder := xml.NewDecoder(bytes.NewBuffer(packetPacket))

	token, err := decoder.Token()
	if err != nil {
		return err
	}

	return element.decodeToken(decoder, token.(xml.StartElement))
}

func (element *Element) decodeToken(decoder *xml.Decoder, startElement xml.StartElement) error {
	element.Name = startElement.Name.Local

	element.Attributes = make(map[string]string)
	for _, attr := range startElement.Attr {
		element.Attributes[attr.Name.Local] = attr.Value
	}

	done := false

	for !done {
		aToken, err := decoder.Token()

		if err != nil {
			return err
		}

		switch token := aToken.(type) {
		case xml.EndElement:
			done = true

		case xml.StartElement:
			var childMessageElement Element

			err = childMessageElement.decodeToken(decoder, token)
			if err != nil {
				return err
			}

			element.Children = append(element.Children, childMessageElement)

		case xml.CharData:
			element.Children = append(element.Children, string(token))
		}
	}

	return nil
}
