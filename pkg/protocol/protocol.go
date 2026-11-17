package protocol

import (
	"encoding/binary"
	"errors"
)

// Comandos do nosso protocolo (1 byte cada)
const (
	CmdConnect   byte = 0x01
	CmdPublish   byte = 0x02
	CmdSubscribe byte = 0x03
	CmdDisconnect byte = 0x04
	CmdAck       byte = 0x05
	CmdNack      byte = 0x06
)

// Message é a estrutura que representa uma mensagem no nosso protocolo.
type Message struct {
	Command byte
	Topic   string
	Payload []byte
}

// Serialize transforma uma Message em um array de bytes ([]byte).
// Formato binário:
// [1 byte comando] + [2 bytes tamanho do tópico] + [X bytes tópico] + [4 bytes tamanho payload] + [Y bytes payload]
func (m *Message) Serialize() []byte {
	topicLen := len(m.Topic)
	payloadLen := len(m.Payload)

	// Aloca o buffer do tamanho exato necessário
	buf := make([]byte, 1+2+topicLen+4+payloadLen)

	// Byte 0: Comando
	buf[0] = m.Command

	// Bytes 1-2: Tamanho do tópico (uint16, big-endian)
	binary.BigEndian.PutUint16(buf[1:3], uint16(topicLen))

	// Bytes 3 até (3+topicLen): O tópico em si
	copy(buf[3:3+topicLen], m.Topic)

	// Bytes (3+topicLen) até (7+topicLen): Tamanho do payload (uint32, big-endian)
	payloadStart := 3 + topicLen
	binary.BigEndian.PutUint32(buf[payloadStart:payloadStart+4], uint32(payloadLen))

	// Bytes (7+topicLen) até o final: O payload
	copy(buf[payloadStart+4:], m.Payload)

	return buf
}

// Deserialize pega um array de bytes e transforma de volta em uma Message.
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

	return &Message{
		Command: cmd,
		Topic:   topic,
		Payload: payload,
	}, nil
}
