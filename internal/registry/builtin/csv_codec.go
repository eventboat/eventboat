package builtin

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strconv"
	"sync"

	"github.com/eventboat/eventboat/internal/registry"
)

// The csv codec maps one message to one CSV record (the file source emits
// per line, so each message is a row). Column names come either from an
// explicit `columns` list (with optional types) or, when `header: true`,
// from the first record the instance decodes — the common "file whose first
// line is a header row" case. Encode always emits data rows (never a
// header). CEL type mapping: string/int/float/bool columns decode to the
// corresponding CEL types (docs/codecs.md).

type csvColumnSpec struct {
	Name string `json:"name" schema:"minLen=1"`
	Type string `json:"type" schema:"enum=string|int|float|bool,default=string"`
}

type csvCodecConfig struct {
	Columns []csvColumnSpec `json:"columns" schema:"optional,minItems=1"`
	Header  bool            `json:"header" schema:"default=false,desc=the first record this instance decodes defines the column names (all string typed)"`
}

func csvCoercer(name, typ string) (func(string) (any, error), error) {
	switch typ {
	case "string":
		return func(s string) (any, error) { return s, nil }, nil
	case "int":
		return func(s string) (any, error) {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("csv codec: column %q: %q is not an int", name, s)
			}
			return n, nil
		}, nil
	case "float":
		return func(s string) (any, error) {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return nil, fmt.Errorf("csv codec: column %q: %q is not a float", name, s)
			}
			return f, nil
		}, nil
	case "bool":
		return func(s string) (any, error) {
			b, err := strconv.ParseBool(s)
			if err != nil {
				return nil, fmt.Errorf("csv codec: column %q: %q is not a bool", name, s)
			}
			return b, nil
		}, nil
	}
	return nil, fmt.Errorf("csv codec: column %q: unsupported type %q", name, typ)
}

func registerCSVCodec(reg *registry.Registry) error {
	return registry.RegisterCodecT(reg, "csv", 1, func(c csvCodecConfig, _ string) (registry.Codec, error) {
		out := &csvCodec{header: c.Header}
		for _, col := range c.Columns {
			coerce, err := csvCoercer(col.Name, col.Type)
			if err != nil {
				return nil, err
			}
			out.columns = append(out.columns, csvColumn{name: col.Name, typ: col.Type, coerce: coerce})
		}
		if out.header && len(out.columns) > 0 {
			return nil, fmt.Errorf("csv codec: set either columns or header, not both")
		}
		if !out.header && len(out.columns) == 0 {
			return nil, fmt.Errorf("csv codec: set columns or header: true")
		}
		return out, nil
	})
}

type csvColumn struct {
	name   string
	typ    string
	coerce func(string) (any, error)
}

type csvCodec struct {
	columns []csvColumn
	header  bool

	mu          sync.Mutex
	headerSeen  bool     // header mode: whether the header row was consumed
	headerNames []string // header mode: column names from that row
}

func (c *csvCodec) Decode(raw []byte) (any, error) {
	rec, err := readCSVRecord(raw)
	if err != nil {
		return nil, fmt.Errorf("csv decode: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.header && !c.headerSeen {
		c.headerSeen = true
		c.headerNames = rec
		return map[string]any{}, nil // the header row itself carries no data
	}
	names := c.headerNames
	if !c.header {
		names = nil
		for _, col := range c.columns {
			names = append(names, col.name)
		}
	}
	if len(rec) != len(names) {
		return nil, fmt.Errorf("csv decode: record has %d fields, expected %d", len(rec), len(names))
	}
	out := make(map[string]any, len(rec))
	for i, field := range rec {
		if c.header { // header mode: everything stays a string
			out[names[i]] = field
			continue
		}
		v, err := c.columns[i].coerce(field)
		if err != nil {
			return nil, err
		}
		out[names[i]] = v
	}
	return out, nil
}

func (c *csvCodec) Encode(v any) ([]byte, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("csv encode: value must be a mapping, got %T", v)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.columns) == 0 && len(c.headerNames) == 0 {
		return nil, fmt.Errorf("csv encode: no column order known (set columns, or decode a header row first)")
	}
	var names []string
	if len(c.columns) > 0 {
		for _, col := range c.columns {
			names = append(names, col.name)
		}
	} else {
		names = c.headerNames
	}
	rec := make([]string, len(names))
	for i, name := range names {
		val, ok := m[name]
		if !ok {
			return nil, fmt.Errorf("csv encode: missing column %q", name)
		}
		rec[i] = csvString(val)
	}
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write(rec); err != nil {
		return nil, fmt.Errorf("csv encode: %w", err)
	}
	w.Flush()
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func readCSVRecord(raw []byte) ([]string, error) {
	r := csv.NewReader(bytes.NewReader(raw))
	r.FieldsPerRecord = -1 // one record, caller checks the count
	r.TrimLeadingSpace = true
	return r.Read()
}

func csvString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}
