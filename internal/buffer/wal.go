package buffer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"

	"github.com/riverpod/riverpod/internal/message"
	"github.com/google/uuid"
)

const walMagic uint32 = 0x45525631 // "ERV1"

// walSourceStageKey carries Message.SourceStageID through WAL serialization
// inside the metadata map (no format change); it is stripped on decode.
const walSourceStageKey = "__er_source_stage"

// errWALCorrupt marks a record that fails CRC validation or is cut short by
// a torn write. Readers treat it as a damaged WAL tail, not a fatal error.
var errWALCorrupt = errors.New("wal record corrupt")

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
	if src := msg.SourceStageID(); src != "" {
		rec.Metadata[walSourceStageKey] = src
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
	// assemble the record in one buffer so the CRC covers every byte and
	// the write below is a single syscall (crash => torn tail, never a
	// half-written field)
	var buf bytes.Buffer
	buf.Write(header)
	buf.Write(idHeader)
	buf.Write(idBytes)
	buf.Write(rec.Payload)
	buf.Write(meta)
	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, crc32.ChecksumIEEE(buf.Bytes()))
	buf.Write(crc)
	_, err = w.Write(buf.Bytes())
	return err
}

func decodeWALRecord(r io.Reader) (*message.Message, error) {
	crc := crc32.NewIEEE()
	header := make([]byte, 12)
	if _, err := io.ReadFull(r, header); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// partial header: torn write at the tail
			return nil, fmt.Errorf("%w: %v", errWALCorrupt, err)
		}
		return nil, err
	}
	crc.Write(header)
	if binary.BigEndian.Uint32(header[0:4]) != walMagic {
		return nil, fmt.Errorf("%w: invalid magic", errWALCorrupt)
	}
	payloadLen := binary.BigEndian.Uint32(header[4:8])
	metaLen := binary.BigEndian.Uint32(header[8:12])

	idLenBuf := make([]byte, 4)
	if _, err := readFullCRC(r, crc, idLenBuf); err != nil {
		return nil, err
	}
	idLen := binary.BigEndian.Uint32(idLenBuf)
	idBytes := make([]byte, idLen)
	if idLen > 0 {
		if _, err := readFullCRC(r, crc, idBytes); err != nil {
			return nil, err
		}
	}
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := readFullCRC(r, crc, payload); err != nil {
			return nil, err
		}
	}
	metaBytes := make([]byte, metaLen)
	if metaLen > 0 {
		if _, err := readFullCRC(r, crc, metaBytes); err != nil {
			return nil, err
		}
	}
	crcBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, crcBuf); err != nil {
		return nil, fmt.Errorf("%w: %v", errWALCorrupt, err)
	}
	if binary.BigEndian.Uint32(crcBuf) != crc.Sum32() {
		return nil, fmt.Errorf("%w: crc mismatch", errWALCorrupt)
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
	if src, ok := metadata[walSourceStageKey].(string); ok {
		delete(metadata, walSourceStageKey)
		msg.SetSourceStageID(src)
	}
	return msg, nil
}

// readFullCRC reads buf and feeds it into the running record CRC. A short
// read means the record was torn by a crash mid-write.
func readFullCRC(r io.Reader, crc hash.Hash32, buf []byte) (int, error) {
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return n, fmt.Errorf("%w: %v", errWALCorrupt, err)
	}
	_, _ = crc.Write(buf)
	return n, nil
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
