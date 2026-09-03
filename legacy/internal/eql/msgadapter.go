package eql

import "github.com/riverpod/riverpod/internal/message"

// MsgAdapter implements MessageView for a *message.Message.
type MsgAdapter struct{ *message.Message }

func NewMsgAdapter(msg *message.Message) MsgAdapter {
	return MsgAdapter{Message: msg}
}

func (m MsgAdapter) EnsureWritable()          { m.Message.EnsureWritable() }
func (m MsgAdapter) SetParsedData(v any)      { m.Message.SetParsedData(v) }
func (m MsgAdapter) Metadata() map[string]any { return m.Message.Metadata }

// PayloadMap returns parsed payload as a map, decoding JSON when needed.
func PayloadMap(msg *message.Message) map[string]any {
	if msg.ParsedData() != nil {
		if m, ok := msg.ParsedData().(map[string]any); ok {
			return m
		}
	}
	return map[string]any{}
}
