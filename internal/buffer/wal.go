package buffer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/edgesets/edgestream/internal/message"
	"github.com/google/uuid"
)

const walMagic uint32 = 0x45525631 // "ERV1"

type walRecord struct {
	ID       string
	Payload  []byte
	Metadata map[string]any
}

func encodeWALRecord(w io.Writer, msg *message.Message) error {
	rec := walRecord{
		ID:       msg.ID,
		Payload:  append([]byte(nil), msg.Payload...),
		Metadata: cloneMetadata(msg.Metadata),
	}
	if rec.ID == "" {
		rec.ID = uuid.NewString()
	}
	meta, err := json.Marshal(rec.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	header := make([]byte, 12)
	binary.BigEndian.PutUint32(header[0:4], walMagic)
	binary.BigEndian.PutUint32(header[4:8], uint32(len(rec.Payload)))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(meta)))
	idBytes := []byte(rec.ID)
	idHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(idHeader, uint32(len(idBytes)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	if _, err := w.Write(idHeader); err != nil {
		return err
	}
	if _, err := w.Write(idBytes); err != nil {
		return err
	}
	if len(rec.Payload) > 0 {
		if _, err := w.Write(rec.Payload); err != nil {
			return err
		}
	}
	if len(meta) > 0 {
		if _, err := w.Write(meta); err != nil {
			return err
		}
	}
	return nil
}

func decodeWALRecord(r io.Reader) (*message.Message, error) {
	header := make([]byte, 12)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(header[0:4]) != walMagic {
		return nil, fmt.Errorf("invalid wal magic")
	}
	payloadLen := binary.BigEndian.Uint32(header[4:8])
	metaLen := binary.BigEndian.Uint32(header[8:12])

	idLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, idLenBuf); err != nil {
		return nil, err
	}
	idLen := binary.BigEndian.Uint32(idLenBuf)
	idBytes := make([]byte, idLen)
	if idLen > 0 {
		if _, err := io.ReadFull(r, idBytes); err != nil {
			return nil, err
		}
	}
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}
	metaBytes := make([]byte, metaLen)
	if metaLen > 0 {
		if _, err := io.ReadFull(r, metaBytes); err != nil {
			return nil, err
		}
	}
	var metadata map[string]any
	if metaLen > 0 {
		if err := json.Unmarshal(metaBytes, &metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}
	if metadata == nil {
		metadata = make(map[string]any)
	}
	msg := message.New(payload, metadata)
	msg.ID = string(idBytes)
	return msg, nil
}

func cloneMetadata(src map[string]any) map[string]any {
	if src == nil {
		return make(map[string]any)
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
