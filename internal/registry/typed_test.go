package registry

import (
	"context"
	"strings"
	"testing"
)

// --- schema generation ---

func TestTypedSchemaKafkaShaped(t *testing.T) {
	type cfg struct {
		Brokers []string `json:"brokers" schema:"minItems=1,desc=Kafka broker addresses"`
		Topics  []string `json:"topics" schema:"minItems=1"`
		GroupID string   `json:"group_id" schema:"default=eventboat,desc=Consumer group id"`
	}
	plan, err := newTypePlan[cfg]("kafka")
	if err != nil {
		t.Fatal(err)
	}
	want := `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["brokers", "topics"],
  "properties": {
    "brokers": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "minItems": 1,
      "description": "Kafka broker addresses"
    },
    "topics": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "minItems": 1
    },
    "group_id": {
      "type": "string",
      "default": "eventboat",
      "description": "Consumer group id"
    }
  },
  "additionalProperties": false
}`
	if plan.schema != want {
		t.Errorf("schema mismatch:\n got: %s\nwant: %s", plan.schema, want)
	}
}

func TestTypedSchemaEmptyStruct(t *testing.T) {
	type cfg struct{}
	plan, err := newTypePlan[cfg]("drop")
	if err != nil {
		t.Fatal(err)
	}
	want := `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false
}`
	if plan.schema != want {
		t.Errorf("schema mismatch:\n got: %s\nwant: %s", plan.schema, want)
	}
}

func TestTypedSchemaScalarsMapsAndNested(t *testing.T) {
	type cursor struct {
		Column string `json:"column" schema:"minLen=1"`
	}
	type cfg struct {
		Driver string            `json:"driver" schema:"enum=mysql|postgres|sqlite,desc=database driver"`
		Args   map[string]any    `json:"args" schema:"optional,desc=named bindings"`
		Labels map[string]string `json:"labels" schema:"optional"`
		Cursor *cursor           `json:"cursor" schema:"optional,desc=watermark column"`
		Ratio  float64           `json:"ratio" schema:"min=0,max=1,optional"`
		Count  int64             `json:"count" schema:"min=1,default=7"`
		Off    bool              `json:"off" schema:"default=true"`
	}
	plan, err := newTypePlan[cfg]("mixed")
	if err != nil {
		t.Fatal(err)
	}
	want := `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["driver"],
  "properties": {
    "driver": {
      "type": "string",
      "enum": ["mysql", "postgres", "sqlite"],
      "description": "database driver"
    },
    "args": {
      "type": "object",
      "description": "named bindings"
    },
    "labels": {
      "type": "object",
      "additionalProperties": {
        "type": "string"
      }
    },
    "cursor": {
      "type": "object",
      "required": ["column"],
      "properties": {
        "column": {
          "type": "string",
          "minLength": 1
        }
      },
      "additionalProperties": false,
      "description": "watermark column"
    },
    "ratio": {
      "type": "number",
      "minimum": 0,
      "maximum": 1
    },
    "count": {
      "type": "integer",
      "minimum": 1,
      "default": 7
    },
    "off": {
      "type": "boolean",
      "default": true
    }
  },
  "additionalProperties": false
}`
	if plan.schema != want {
		t.Errorf("schema mismatch:\n got: %s\nwant: %s", plan.schema, want)
	}
}

func TestTypedSchemaSliceOfStruct(t *testing.T) {
	type column struct {
		Name string `json:"name"`
		Type string `json:"type" schema:"enum=string|int,default=string"`
	}
	type cfg struct {
		Columns []column `json:"columns" schema:"optional,minItems=1"`
	}
	plan, err := newTypePlan[cfg]("csv")
	if err != nil {
		t.Fatal(err)
	}
	want := `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "columns": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["name"],
        "properties": {
          "name": {
            "type": "string"
          },
          "type": {
            "type": "string",
            "enum": ["string", "int"],
            "default": "string"
          }
        },
        "additionalProperties": false
      },
      "minItems": 1
    }
  },
  "additionalProperties": false
}`
	if plan.schema != want {
		t.Errorf("schema mismatch:\n got: %s\nwant: %s", plan.schema, want)
	}
}

func TestTypedSharedNestedTypeAllowed(t *testing.T) {
	// Two sibling fields of the same nested type are fine; only a type that
	// contains itself (directly or transitively) is recursive.
	type window struct {
		Column string `json:"column" schema:"optional,minLen=1"`
	}
	type cfg struct {
		From  window `json:"from" schema:"optional"`
		To    window `json:"to" schema:"optional"`
		Pairs []struct {
			A string `json:"a"`
			B string `json:"b"`
		} `json:"pairs" schema:"optional"`
	}
	if _, err := newTypePlan[cfg]("x"); err != nil {
		t.Errorf("shared nested type rejected: %v", err)
	}
}

func TestTypedRecursiveConfigRejected(t *testing.T) {
	type node struct {
		Name string `json:"name"`
		Next *node  `json:"next" schema:"optional"`
	}
	if _, err := newTypePlan[node]("x"); err == nil || !strings.Contains(err.Error(), "recursive config type") {
		t.Errorf("recursive type error = %v", err)
	}
}

func TestTypedUnsupportedTypesRejected(t *testing.T) {
	type badChan struct {
		Ch chan int `json:"ch"`
	}
	if _, err := newTypePlan[badChan]("x"); err == nil || !strings.Contains(err.Error(), "unsupported config field type") {
		t.Errorf("chan field error = %v", err)
	}
	type badMap struct {
		M map[int]string `json:"m"`
	}
	if _, err := newTypePlan[badMap]("x"); err == nil || !strings.Contains(err.Error(), "map keys must be strings") {
		t.Errorf("int-keyed map error = %v", err)
	}
	type badMapVal struct {
		M map[string]int64 `json:"m"`
	}
	if _, err := newTypePlan[badMapVal]("x"); err == nil || !strings.Contains(err.Error(), "map values must be string or any") {
		t.Errorf("int-valued map error = %v", err)
	}
	// Scalar roots are legal (transform configs, e.g. the script plugin's
	// source text); unsupported root kinds are not.
	if _, err := newTypePlan[string]("x"); err != nil {
		t.Errorf("scalar top error = %v", err)
	}
	if _, err := newTypePlan[chan int]("x"); err == nil || !strings.Contains(err.Error(), "unsupported config field type") {
		t.Errorf("chan top error = %v", err)
	}
}

func TestTypedScalarRootPlan(t *testing.T) {
	plan, err := newTypePlan[string]("script")
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if !strings.Contains(plan.schema, `"type": "string"`) {
		t.Errorf("schema = %s", plan.schema)
	}
	got, err := decodeTyped[string]("payload.x = 1", plan.defaults)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != "payload.x = 1" {
		t.Errorf("decoded = %q", got)
	}
}

func TestTypedBadTagsRejected(t *testing.T) {
	type enumOnInt struct {
		F int `json:"f" schema:"enum=a|b"`
	}
	if _, err := newTypePlan[enumOnInt]("x"); err == nil || !strings.Contains(err.Error(), "enum= applies to string fields") {
		t.Errorf("enum on int error = %v", err)
	}
	type minOnString struct {
		F string `json:"f" schema:"min=3"`
	}
	if _, err := newTypePlan[minOnString]("x"); err == nil || !strings.Contains(err.Error(), "min= applies to numeric fields") {
		t.Errorf("min on string error = %v", err)
	}
	type minItemsOnString struct {
		F string `json:"f" schema:"minItems=3"`
	}
	if _, err := newTypePlan[minItemsOnString]("x"); err == nil || !strings.Contains(err.Error(), "minItems= applies to array fields") {
		t.Errorf("minItems on string error = %v", err)
	}
	type unknownKey struct {
		F string `json:"f" schema:"typo=1"`
	}
	if _, err := newTypePlan[unknownKey]("x"); err == nil || !strings.Contains(err.Error(), "unknown schema tag key") {
		t.Errorf("unknown key error = %v", err)
	}
	type badDefault struct {
		F int `json:"f" schema:"default=soon"`
	}
	if _, err := newTypePlan[badDefault]("x"); err == nil || !strings.Contains(err.Error(), "is not a valid number") {
		t.Errorf("bad default error = %v", err)
	}
	type badNumber struct {
		F int `json:"f" schema:"min=soon"`
	}
	if _, err := newTypePlan[badNumber]("x"); err == nil || !strings.Contains(err.Error(), "is not a number") {
		t.Errorf("bad min error = %v", err)
	}
}

func TestTypedDescMayContainCommas(t *testing.T) {
	type cfg struct {
		Path string `json:"path" schema:"minLen=1,desc=file to tail, one message per line"`
	}
	plan, err := newTypePlan[cfg]("file")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.schema, `"description": "file to tail, one message per line"`) {
		t.Errorf("comma description mangled: %s", plan.schema)
	}
}

// --- decode ---

type decodeNested struct {
	Name string `json:"name"`
	Type string `json:"type" schema:"enum=string|int,default=string"`
}

type decodeCfg struct {
	Brokers []string       `json:"brokers"`
	GroupID string         `json:"group_id" schema:"default=eventboat"`
	PollMS  int            `json:"poll_every_ms" schema:"default=250"`
	Enabled bool           `json:"enabled" schema:"default=true"`
	Columns []decodeNested `json:"columns" schema:"optional"`
}

func TestDecodeTypedAppliesDefaults(t *testing.T) {
	plan, err := newTypePlan[decodeCfg]("x")
	if err != nil {
		t.Fatal(err)
	}
	c, err := decodeTyped[decodeCfg](map[string]any{
		"brokers": []any{"a:9092"},
		"columns": []any{map[string]any{"name": "ts"}, map[string]any{"name": "v", "type": "int"}},
	}, plan.defaults)
	if err != nil {
		t.Fatal(err)
	}
	if c.GroupID != "eventboat" || c.PollMS != 250 || !c.Enabled {
		t.Errorf("defaults not applied: %+v", c)
	}
	if len(c.Columns) != 2 || c.Columns[0].Type != "string" || c.Columns[1].Type != "int" {
		t.Errorf("nested slice defaults not applied: %+v", c.Columns)
	}
	// Explicit values win over defaults.
	c2, err := decodeTyped[decodeCfg](map[string]any{"brokers": nil, "group_id": "g"}, plan.defaults)
	if err != nil {
		t.Fatal(err)
	}
	if c2.GroupID != "g" {
		t.Errorf("explicit value overridden: %+v", c2)
	}
}

func TestDecodeTypedNilAndEmpty(t *testing.T) {
	plan, err := newTypePlan[decodeCfg]("x")
	if err != nil {
		t.Fatal(err)
	}
	c, err := decodeTyped[decodeCfg](nil, plan.defaults)
	if err != nil {
		t.Fatal(err)
	}
	if c.GroupID != "eventboat" {
		t.Errorf("nil cfg defaults = %+v", c)
	}
}

func TestDecodeTypedRejectsUnknownAndMistyped(t *testing.T) {
	plan, err := newTypePlan[decodeCfg]("x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeTyped[decodeCfg](map[string]any{"brokers": nil, "nope": 1}, plan.defaults); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Errorf("unknown field error = %v", err)
	}
	if _, err := decodeTyped[decodeCfg](map[string]any{"brokers": []any{"a"}, "poll_every_ms": "fast"}, plan.defaults); err == nil {
		t.Error("string in int field accepted")
	}
}

// --- end-to-end registration ---

type demoSource struct{}

func (d *demoSource) Init([]byte) error                             { return nil }
func (d *demoSource) Run(context.Context, func(Message))            {}
func (d *demoSource) Commit(context.Context, int64) ([]byte, error) { return nil, nil }
func (d *demoSource) Close() error                                  { return nil }

func TestRegisterSourceT(t *testing.T) {
	type cfg struct {
		Topic   string `json:"topic"`
		GroupID string `json:"group_id" schema:"default=demo-group"`
	}
	r := New()
	var got cfg
	err := RegisterSourceT(r, "demo", 1, []string{"pull"}, func(c cfg) (Source, error) {
		got = c
		return &demoSource{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	src, err := r.NewSource("demo", map[string]any{"topic": "t"})
	if err != nil {
		t.Fatal(err)
	}
	_ = src.Close()
	if got.Topic != "t" || got.GroupID != "demo-group" {
		t.Errorf("factory config = %+v", got)
	}
	meta, ok := r.LookupSource("demo")
	if !ok || !strings.Contains(meta.Schema, `"default": "demo-group"`) {
		t.Errorf("catalog schema = %v %s", ok, meta.Schema)
	}
	if len(meta.Capabilities) != 1 || meta.Capabilities[0] != "pull" {
		t.Errorf("capabilities = %v", meta.Capabilities)
	}

	// Schema gates: missing required, enum violation, unknown field.
	if _, err := r.NewSource("demo", map[string]any{}); err == nil {
		t.Error("missing required accepted")
	}
	if err := RegisterSourceT(r, "demo", 1, nil, func(c cfg) (Source, error) { return nil, nil }); err == nil {
		t.Error("duplicate accepted")
	}
	if err := RegisterSourceT[Source, cfg](r, "nilfac", 1, nil, nil); err == nil || !strings.Contains(err.Error(), "nil factory") {
		t.Errorf("nil factory error = %v", err)
	}
}

func TestRegisterSinkTAndCodecT(t *testing.T) {
	type sinkCfg struct {
		Path string `json:"path" schema:"minLen=1"`
	}
	r := New()
	if err := RegisterSinkT(r, "sinkt", 1, func(c sinkCfg) (Sink, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.NewSink("sinkt", map[string]any{"path": ""}); err == nil {
		t.Error("minLength violation accepted")
	}

	type codecCfg struct {
		Set string `json:"descriptor_set" schema:"minLen=1"`
	}
	if err := RegisterCodecT(r, "codect", 1, func(c codecCfg, dir string) (Codec, error) {
		if dir == "" {
			t.Error("dir not forwarded")
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.NewCodec("codect", nil, "somedir"); err == nil {
		t.Error("missing required accepted")
	}
	if _, err := r.NewCodec("codect", map[string]any{"descriptor_set": "a.pb"}, "somedir"); err != nil {
		t.Errorf("valid codec config rejected: %v", err)
	}
}
