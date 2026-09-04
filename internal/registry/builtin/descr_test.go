package builtin

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// The committed example.descr is BUILT BY THIS TEST'S GENERATOR, not by
// protoc (CI runners do not ship protoc, and a generator in Go is
// byte-deterministic across environments). testdata/example.proto is the
// human-readable mirror — keep the two in sync when changing the fixture.
var updateDescr = flag.Bool("update-descr", false, "regenerate testdata/example.descr from the Go generator")

// buildExampleDescriptorSet is the source of truth for the protobuf codec's
// test fixture:
//
//	syntax = "proto3"; package eventboat.example;
//	message Metric { string name = 1; int64 value = 2; repeated string tags = 3; }
func buildExampleDescriptorSet() []byte {
	str := func(s string) *string { return &s }
	msg := &descriptorpb.DescriptorProto{
		Name: str("Metric"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: str("name"), Number: proto.Int32(1), Label: label(1), Type: ftype(9)},   // TYPE_STRING
			{Name: str("value"), Number: proto.Int32(2), Label: label(1), Type: ftype(3)},  // TYPE_INT64
			{Name: str("tags"), Number: proto.Int32(3), Label: label(3), Type: ftype(9)},   // repeated TYPE_STRING
		},
	}
	file := &descriptorpb.FileDescriptorProto{
		Name:    str("example.proto"),
		Package: str("eventboat.example"),
		Syntax:  str("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{msg},
	}
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{file}}
	out, err := proto.Marshal(set)
	if err != nil {
		panic(err)
	}
	return out
}

func label(v int32) *descriptorpb.FieldDescriptorProto_Label {
	l := descriptorpb.FieldDescriptorProto_Label(v)
	return &l
}

func ftype(v int32) *descriptorpb.FieldDescriptorProto_Type {
	t := descriptorpb.FieldDescriptorProto_Type(v)
	return &t
}

// TestExampleDescriptorMatchesGenerator pins the committed artifact to the
// generator: run with -update-descr after changing the fixture.
func TestExampleDescriptorMatchesGenerator(t *testing.T) {
	path := filepath.Join("testdata", "example.descr")
	if *updateDescr {
		if err := os.WriteFile(path, buildExampleDescriptorSet(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	committed, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("committed descriptor missing: %v (run go test ./internal/registry/builtin -update-descr)", err)
	}
	if want := buildExampleDescriptorSet(); !bytes.Equal(committed, want) {
		t.Errorf("example.descr does not match the Go generator — regenerate with: go test ./internal/registry/builtin -update-descr")
	}
}
