package protocol

import (
	"encoding/binary"
	"errors"
)

// Comandos do nosso protocolo (1 byte cada)
const (
	CmdConnect      byte = 0x01
	CmdPublish      byte = 0x02
	CmdSubscribe    byte = 0x03
	CmdDisconnect   byte = 0x04
	CmdAck          byte = 0x05
	CmdNack         byte = 0x06
	CmdCreateTopic  byte = 0x07
	CmdRemoveTopic  byte = 0x08
	CmdUnsubscribe  byte = 0x09
)

type Message struct {
	Command byte
	Topic   string
	Payload []byte
}

func (m *Message) Serialize() []byte {
	topicLen := len(m.Topic)
	payloadLen := len(m.Payload)
	buf := make([]byte, 1+2+topicLen+4+payloadLen)
	buf[0] = m.Command
	binary.BigEndian.PutUint16(buf[1:3], uint16(topicLen))
	copy(buf[3:3+topicLen], m.Topic)
	payloadStart := 3 + topicLen
	binary.BigEndian.PutUint32(buf[payloadStart:payloadStart+4], uint32(payloadLen))
	copy(buf[payloadStart+4:], m.Payload)
	return buf
}

func Deserialize(data []byte) (*Message, error) {
	if len(data) < 3 {
		return nil, errors.New("pacote muito pequeno")
	}
	cmd := data[0]
	topicLen := int(binary.BigEndian.Uint16(data[1:3]))
	if len(data) < 3+topicLen+4 {
		return nil, errors.New("pacote incompleto para o tópico")
	}
	topic := string(data[3 : 3+topicLen])
	payloadStart := 3 + topicLen
	payloadLen := int(binary.BigEndian.Uint32(data[payloadStart : payloadStart+4]))
	if len(data) < payloadStart+4+payloadLen {
		return nil, errors.New("pacote incompleto para o payload")
	}
	payload := data[payloadStart+4 : payloadStart+4+payloadLen]
	return &Message{Command: cmd, Topic: topic, Payload: payload}, nil
}
