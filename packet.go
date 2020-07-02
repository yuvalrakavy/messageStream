package messageStream

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
)

type Element struct {
	Name       string
	Attributes Attributes
	Children   []interface{}
}

type Packet struct {
	Element  *Element
	EndPoint *EndPoint
}

type BroadcastPacket struct {
	Element *Element
	Service *Service
}

type Attributes map[string]interface{}

func NewElement(name string, content ...interface{}) *Element {
	element := Element{Name: name, Attributes: make(Attributes)}

	element.addItems(content)

	return &element
}

func (element *Element) Add(content ...interface{}) *Element {
	return element.addItems(content)
}

func (element *Element) addItems(content []interface{}) *Element {
	for _, itemObject := range content {
		switch item := itemObject.(type) {
		case Attributes:
			element.SetAttributes(item)

		case Element:
			element.Children = append(element.Children, &item)

		case *Element:
			element.Children = append(element.Children, item)

		case []interface{}:
			element.addItems(item)

		default:
			element.Children = append(element.Children, toString(item))
		}
	}

	return element
}

func (element Element) String() string {
	buffer := bytes.NewBuffer(nil)
	xmlEncoder := xml.NewEncoder(buffer)

	err := element.encodeElement(xmlEncoder)
	xmlEncoder.Flush()

	if err != nil {
		return "Error: " + err.Error()
	} else {
		return buffer.String()
	}
}

func (pakcet Packet) String() string {
	var endPoint string

	if pakcet.EndPoint == nil {
		endPoint = "*Broadcast*"
	} else {
		endPoint = pakcet.EndPoint.String()
	}

	return "endPoint: " + endPoint + ": " + pakcet.Element.String()
}

func (packet BroadcastPacket) String() string {
	return "*Broadcast* - " + packet.Element.String()
}

func (element *Element) GetAttribute(name string, defaultValue string) string {
	value, hasValue := element.Attributes[name]

	if hasValue {
		return toString(value)
	} else {
		return defaultValue
	}
}

func (element *Element) GetIntAttribute(name string, defaultValue int) int {
	value, hasValue := element.Attributes[name]

	if hasValue {
		value, err := strconv.Atoi(toString(value))

		if err != nil {
			panic(fmt.Sprintf("Element %#v - Attribute %v has value which is not valid integer: %v", element, name, value))
		}

		return value
	} else {
		return defaultValue
	}
}

func (element *Element) GetBoolAttribute(name string, defaultValue bool) bool {
	value, hasValue := element.Attributes[name]

	if hasValue {
		valueAsString := toString(value)

		if strings.EqualFold(valueAsString, "true") {
			return true
		} else if strings.EqualFold(valueAsString, "false") {
			return false
		} else {
			panic(fmt.Sprintf("Element %#v - Attribute %v has value which is not valid boolean: %v", element, name, valueAsString))
		}
	} else {
		return defaultValue
	}
}

func (element *Element) SetAttributes(setMap map[string]interface{}) {
	for name, v := range setMap {
		element.Attributes[name] = toString(v)
	}
}

func (element *Element) SetAttribute(name string, value interface{}) {
	element.SetAttributes(Attributes{name: value})
}

func (element *Element) RemoveAttribute(name string) {
	delete(element.Attributes, name)
}

func toString(v interface{}) string {
	switch value := v.(type) {
	case string:
		return value
	case int:
		return strconv.Itoa(value)

	case bool:
		if value {
			return "true"
		} else {
			return "false"
		}

	case fmt.Stringer:
		return value.String()

	default:
		panic(fmt.Sprintf("Cannot convert %v (type %T) to string", value, value))

	}
}

func (element *Element) GetChildren(name string) []*Element {
	var selectedChildren []*Element

	for _, childObject := range element.Children {
		child, isElement := childObject.(*Element)

		if isElement && child.Name == name {
			selectedChildren = append(selectedChildren, child)
		}
	}

	return selectedChildren
}

func (element *Element) GetChild(name string) *Element {
	children := element.GetChildren(name)

	if len(children) > 0 {
		return children[0]
	} else {
		return nil
	}
}

func (element *Element) GetValue() string {
	result := ""

	for _, childObject := range element.Children {
		_, isElement := childObject.(*Element)

		if !isElement {
			result += toString(childObject)
		}
	}

	return result
}
