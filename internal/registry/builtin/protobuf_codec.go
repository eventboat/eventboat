package builtin

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/eventboat/eventboat/internal/registry"
)

// The protobuf codec decodes/encodes one compiled message type described by
// a FileDescriptorSet (protoc --descriptor_set_out). Payloads surface as
// plain map[string]any via protojson, so CEL/Starlark see the same shapes
// the json codec produces. Type mapping: int32/int64/sint/fixed → CEL int,
// uint variants → CEL uint, float/double → CEL double, string/bool direct,
// bytes → base64 string, repeated → list, map/message → map (docs/codecs.md).
//
// descriptor_set paths resolve against the pipeline file's directory (the
// same rule wasm modules follow); the decode path runs protojson → Go, the
// encode path Go → protojson → dynamic message — honest double conversion,
// documented as not the hot path (§redesign-v3-review-m4.md §一).

const protobufCodecSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["descriptor_set", "message"],
  "properties": {
    "descriptor_set": { "type": "string", "minLength": 1, "description": "path to a compiled FileDescriptorSet (.pb), relative to the pipeline file" },
    "message": { "type": "string", "minLength": 1, "description": "fully-qualified message name, e.g. com.example.Order" }
  },
  "additionalProperties": false
}`

func registerProtobufCodec(reg *registry.Registry) error {
	return reg.RegisterCodec("protobuf", 1, protobufCodecSchema, func(cfg map[string]any, dir string) (registry.Codec, error) {
		setPath, _ := cfg["descriptor_set"].(string)
		msgName, _ := cfg["message"].(string)
		if setPath == "" || msgName == "" {
			return nil, fmt.Errorf("protobuf codec: descriptor_set and message are required")
		}
		if !filepath.IsAbs(setPath) && dir != "" {
			setPath = filepath.Join(dir, setPath)
		}
		data, err := os.ReadFile(setPath)
		if err != nil {
			return nil, fmt.Errorf("protobuf codec: read descriptor_set: %w", err)
		}
		var set descriptorpb.FileDescriptorSet
		if err := proto.Unmarshal(data, &set); err != nil {
			return nil, fmt.Errorf("protobuf codec: %s is not a FileDescriptorSet: %w", setPath, err)
		}
		files := &protoregistry.Files{}
		for _, fd := range set.File {
			// A set produced with --include_imports carries dependencies;
			// register in order, tolerating already-present files.
			if _, err := files.FindFileByPath(fd.GetName()); err == nil {
				continue
			}
			fdesc, err := protodesc.NewFile(fd, files)
			if err != nil {
				return nil, fmt.Errorf("protobuf codec: file %s: %w", fd.GetName(), err)
			}
			if err := files.RegisterFile(fdesc); err != nil {
				return nil, fmt.Errorf("protobuf codec: register %s: %w", fd.GetName(), err)
			}
		}
		desc, err := files.FindDescriptorByName(protoreflect.FullName(msgName))
		if err != nil {
			return nil, fmt.Errorf("protobuf codec: message %q not found in the descriptor set: %w", msgName, err)
		}
		md, ok := desc.(protoreflect.MessageDescriptor)
		if !ok {
			return nil, fmt.Errorf("protobuf codec: %q is not a message", msgName)
		}
		return &protobufCodec{desc: md}, nil
	})
}

type protobufCodec struct {
	desc protoreflect.MessageDescriptor
}

func (c *protobufCodec) Decode(raw []byte) (any, error) {
	m := dynamicpb.NewMessage(c.desc)
	if err := proto.Unmarshal(raw, m); err != nil {
		return nil, fmt.Errorf("protobuf decode: %w", err)
	}
	js, err := protojson.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("protobuf decode: %w", err)
	}
	var v any
	if err := json.Unmarshal(js, &v); err != nil {
		return nil, fmt.Errorf("protobuf decode: %w", err)
	}
	return v, nil
}

func (c *protobufCodec) Encode(v any) ([]byte, error) {
	js, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("protobuf encode: %w", err)
	}
	m := dynamicpb.NewMessage(c.desc)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(js, m); err != nil {
		return nil, fmt.Errorf("protobuf encode: %w", err)
	}
	out, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("protobuf encode: %w", err)
	}
	return out, nil
}
