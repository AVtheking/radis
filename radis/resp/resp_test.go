package resp

import (
	"bufio"
	"strings"
	"testing"
)

func newReader(s string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(s))
}

// ==================== Parsing: Simple Strings ====================

func TestParseSimpleString(t *testing.T) {
	val, err := ParseRESP(newReader("+OK\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != SimpleString {
		t.Fatalf("expected SimpleString, got %v", val.Type)
	}
	if val.Str != "OK" {
		t.Errorf("expected %q, got %q", "OK", val.Str)
	}
}

func TestParseSimpleStringWithSpaces(t *testing.T) {
	val, err := ParseRESP(newReader("+hello world\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != SimpleString {
		t.Fatalf("expected SimpleString, got %v", val.Type)
	}
	if val.Str != "hello world" {
		t.Errorf("expected %q, got %q", "hello world", val.Str)
	}
}

func TestParseEmptySimpleString(t *testing.T) {
	val, err := ParseRESP(newReader("+\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != SimpleString {
		t.Fatalf("expected SimpleString, got %v", val.Type)
	}
	if val.Str != "" {
		t.Errorf("expected empty string, got %q", val.Str)
	}
}

// ==================== Parsing: Errors ====================

func TestParseError(t *testing.T) {
	val, err := ParseRESP(newReader("-ERR unknown command\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != Error {
		t.Fatalf("expected Error, got %v", val.Type)
	}
	if val.Str != "ERR unknown command" {
		t.Errorf("expected %q, got %q", "ERR unknown command", val.Str)
	}
}

func TestParseErrorShort(t *testing.T) {
	val, err := ParseRESP(newReader("-WRONGTYPE\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != Error {
		t.Fatalf("expected Error, got %v", val.Type)
	}
	if val.Str != "WRONGTYPE" {
		t.Errorf("expected %q, got %q", "WRONGTYPE", val.Str)
	}
}

// ==================== Parsing: Integers ====================

func TestParseInteger(t *testing.T) {
	val, err := ParseRESP(newReader(":1000\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != Integer {
		t.Fatalf("expected Integer, got %v", val.Type)
	}
	if val.Num != 1000 {
		t.Errorf("expected 1000, got %d", val.Num)
	}
}

func TestParseNegativeInteger(t *testing.T) {
	val, err := ParseRESP(newReader(":-42\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != Integer {
		t.Fatalf("expected Integer, got %v", val.Type)
	}
	if val.Num != -42 {
		t.Errorf("expected -42, got %d", val.Num)
	}
}

func TestParseZeroInteger(t *testing.T) {
	val, err := ParseRESP(newReader(":0\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != Integer {
		t.Fatalf("expected Integer, got %v", val.Type)
	}
	if val.Num != 0 {
		t.Errorf("expected 0, got %d", val.Num)
	}
}

// ==================== Parsing: Bulk Strings ====================

func TestParseBulkString(t *testing.T) {
	val, err := ParseRESP(newReader("$5\r\nhello\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != BulkString {
		t.Fatalf("expected BulkString, got %v", val.Type)
	}
	if val.Str != "hello" {
		t.Errorf("expected %q, got %q", "hello", val.Str)
	}
}

func TestParseBulkStringWithSpaces(t *testing.T) {
	val, err := ParseRESP(newReader("$11\r\nhello world\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != BulkString {
		t.Fatalf("expected BulkString, got %v", val.Type)
	}
	if val.Str != "hello world" {
		t.Errorf("expected %q, got %q", "hello world", val.Str)
	}
}

func TestParseEmptyBulkString(t *testing.T) {
	val, err := ParseRESP(newReader("$0\r\n\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != BulkString {
		t.Fatalf("expected BulkString, got %v", val.Type)
	}
	if val.IsNull {
		t.Error("expected non-null, got null")
	}
	if val.Str != "" {
		t.Errorf("expected empty string, got %q", val.Str)
	}
}

func TestParseNullBulkString(t *testing.T) {
	val, err := ParseRESP(newReader("$-1\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != BulkString {
		t.Fatalf("expected BulkString, got %v", val.Type)
	}
	if !val.IsNull {
		t.Error("expected null bulk string")
	}
}

func TestParseBulkStringWithCRLFInside(t *testing.T) {
	// Bulk strings are binary safe — the length tells you exactly how many bytes to read
	val, err := ParseRESP(newReader("$12\r\nhello\r\nworld\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != BulkString {
		t.Fatalf("expected BulkString, got %v", val.Type)
	}
	if val.Str != "hello\r\nworld" {
		t.Errorf("expected %q, got %q", "hello\r\nworld", val.Str)
	}
}

// ==================== Parsing: Arrays ====================

func TestParseArray(t *testing.T) {
	val, err := ParseRESP(newReader("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != Array {
		t.Fatalf("expected Array, got %v", val.Type)
	}
	if len(val.Array) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(val.Array))
	}
	if val.Array[0].Str != "foo" {
		t.Errorf("element 0: expected %q, got %q", "foo", val.Array[0].Str)
	}
	if val.Array[1].Str != "bar" {
		t.Errorf("element 1: expected %q, got %q", "bar", val.Array[1].Str)
	}

}

func TestParseEmptyArray(t *testing.T) {
	val, err := ParseRESP(newReader("*0\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != Array {
		t.Fatalf("expected Array, got %v", val.Type)
	}
	if val.IsNull {
		t.Error("expected non-null, got null")
	}
	if len(val.Array) != 0 {
		t.Errorf("expected 0 elements, got %d", len(val.Array))
	}
}

func TestParseNullArray(t *testing.T) {
	val, err := ParseRESP(newReader("*-1\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != Array {
		t.Fatalf("expected Array, got %v", val.Type)
	}
	if !val.IsNull {
		t.Error("expected null array")
	}
}

func TestParseMixedTypeArray(t *testing.T) {
	// Array with different types: integer, bulk string, simple string
	input := "*3\r\n:42\r\n$5\r\nhello\r\n+OK\r\n"
	val, err := ParseRESP(newReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != Array {
		t.Fatalf("expected Array, got %v", val.Type)
	}
	if len(val.Array) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(val.Array))
	}
	if val.Array[0].Type != Integer || val.Array[0].Num != 42 {
		t.Errorf("element 0: expected Integer 42, got %v %d", val.Array[0].Type, val.Array[0].Num)
	}
	if val.Array[1].Type != BulkString || val.Array[1].Str != "hello" {
		t.Errorf("element 1: expected BulkString %q, got %v %q", "hello", val.Array[1].Type, val.Array[1].Str)
	}
	if val.Array[2].Type != SimpleString || val.Array[2].Str != "OK" {
		t.Errorf("element 2: expected SimpleString %q, got %v %q", "OK", val.Array[2].Type, val.Array[2].Str)
	}
}

func TestParseNestedArray(t *testing.T) {
	// *2\r\n *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n *1\r\n:99\r\n
	input := "*2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*1\r\n:99\r\n"
	val, err := ParseRESP(newReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != Array {
		t.Fatalf("expected Array, got %v", val.Type)
	}
	if len(val.Array) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(val.Array))
	}

	inner1 := val.Array[0]
	if inner1.Type != Array || len(inner1.Array) != 2 {
		t.Fatalf("element 0: expected Array of 2, got %v len %d", inner1.Type, len(inner1.Array))
	}
	if inner1.Array[0].Str != "foo" || inner1.Array[1].Str != "bar" {
		t.Errorf("inner array: expected [foo, bar], got [%q, %q]", inner1.Array[0].Str, inner1.Array[1].Str)
	}

	inner2 := val.Array[1]
	if inner2.Type != Array || len(inner2.Array) != 1 {
		t.Fatalf("element 1: expected Array of 1, got %v len %d", inner2.Type, len(inner2.Array))
	}
	if inner2.Array[0].Type != Integer || inner2.Array[0].Num != 99 {
		t.Errorf("inner array: expected Integer 99, got %v %d", inner2.Array[0].Type, inner2.Array[0].Num)
	}
}

func TestParseArrayWithNullElement(t *testing.T) {
	// Array containing a null bulk string
	input := "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n"
	val, err := ParseRESP(newReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != Array || len(val.Array) != 3 {
		t.Fatalf("expected Array of 3, got %v len %d", val.Type, len(val.Array))
	}
	if val.Array[0].Str != "foo" {
		t.Errorf("element 0: expected %q, got %q", "foo", val.Array[0].Str)
	}
	if !val.Array[1].IsNull {
		t.Error("element 1: expected null bulk string")
	}
	if val.Array[2].Str != "bar" {
		t.Errorf("element 2: expected %q, got %q", "bar", val.Array[2].Str)
	}
}

// ==================== Serialization ====================

func TestSerializeSimpleString(t *testing.T) {
	val := RESPValue{Type: SimpleString, Str: "OK"}
	got := string(val.Serialize())
	if got != "+OK\r\n" {
		t.Errorf("expected %q, got %q", "+OK\r\n", got)
	}
}

func TestSerializeError(t *testing.T) {
	val := RESPValue{Type: Error, Str: "ERR unknown command"}
	got := string(val.Serialize())
	if got != "-ERR unknown command\r\n" {
		t.Errorf("expected %q, got %q", "-ERR unknown command\r\n", got)
	}
}

func TestSerializeInteger(t *testing.T) {
	val := RESPValue{Type: Integer, Num: 1000}
	got := string(val.Serialize())
	if got != ":1000\r\n" {
		t.Errorf("expected %q, got %q", ":1000\r\n", got)
	}
}

func TestSerializeNegativeInteger(t *testing.T) {
	val := RESPValue{Type: Integer, Num: -42}
	got := string(val.Serialize())
	if got != ":-42\r\n" {
		t.Errorf("expected %q, got %q", ":-42\r\n", got)
	}
}

func TestSerializeBulkString(t *testing.T) {
	val := RESPValue{Type: BulkString, Str: "hello"}
	got := string(val.Serialize())
	if got != "$5\r\nhello\r\n" {
		t.Errorf("expected %q, got %q", "$5\r\nhello\r\n", got)
	}
}

func TestSerializeEmptyBulkString(t *testing.T) {
	val := RESPValue{Type: BulkString, Str: ""}
	got := string(val.Serialize())
	if got != "$0\r\n\r\n" {
		t.Errorf("expected %q, got %q", "$0\r\n\r\n", got)
	}
}

func TestSerializeNullBulkString(t *testing.T) {
	val := RESPValue{Type: BulkString, IsNull: true}
	got := string(val.Serialize())
	if got != "$-1\r\n" {
		t.Errorf("expected %q, got %q", "$-1\r\n", got)
	}
}

func TestSerializeArray(t *testing.T) {
	val := RESPValue{
		Type: Array,
		Array: []RESPValue{
			{Type: BulkString, Str: "foo"},
			{Type: BulkString, Str: "bar"},
		},
	}
	got := string(val.Serialize())
	expected := "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestSerializeEmptyArray(t *testing.T) {
	val := RESPValue{Type: Array, Array: []RESPValue{}}
	got := string(val.Serialize())
	if got != "*0\r\n" {
		t.Errorf("expected %q, got %q", "*0\r\n", got)
	}
}

func TestSerializeNullArray(t *testing.T) {
	val := RESPValue{Type: Array, IsNull: true}
	got := string(val.Serialize())
	if got != "*-1\r\n" {
		t.Errorf("expected %q, got %q", "*-1\r\n", got)
	}
}

// ==================== Round-trip ====================

func TestRoundTripBulkString(t *testing.T) {
	original := RESPValue{Type: BulkString, Str: "hello world"}
	serialized := original.Serialize()
	parsed, err := ParseRESP(newReader(string(serialized)))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Type != original.Type || parsed.Str != original.Str {
		t.Errorf("round-trip failed: got Type=%v Str=%q", parsed.Type, parsed.Str)
	}
}

func TestRoundTripArray(t *testing.T) {
	original := RESPValue{
		Type: Array,
		Array: []RESPValue{
			{Type: SimpleString, Str: "OK"},
			{Type: Integer, Num: 42},
			{Type: BulkString, Str: "data"},
			{Type: BulkString, IsNull: true},
		},
	}
	serialized := original.Serialize()
	parsed, err := ParseRESP(newReader(string(serialized)))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Type != Array || len(parsed.Array) != 4 {
		t.Fatalf("expected Array of 4, got %v len %d", parsed.Type, len(parsed.Array))
	}
	if parsed.Array[0].Str != "OK" {
		t.Errorf("element 0: expected %q, got %q", "OK", parsed.Array[0].Str)
	}
	if parsed.Array[1].Num != 42 {
		t.Errorf("element 1: expected 42, got %d", parsed.Array[1].Num)
	}
	if parsed.Array[2].Str != "data" {
		t.Errorf("element 2: expected %q, got %q", "data", parsed.Array[2].Str)
	}
	if !parsed.Array[3].IsNull {
		t.Error("element 3: expected null")
	}
}

// ==================== Error cases ====================

func TestParseInvalidPrefix(t *testing.T) {
	_, err := ParseRESP(newReader("~invalid\r\n"))
	if err == nil {
		t.Error("expected error for invalid prefix, got nil")
	}
}

func TestParseInvalidIntegerValue(t *testing.T) {
	_, err := ParseRESP(newReader(":notanumber\r\n"))
	if err == nil {
		t.Error("expected error for non-numeric integer, got nil")
	}
}

func TestParseEmptyInput(t *testing.T) {
	_, err := ParseRESP(newReader(""))
	if err == nil {
		t.Error("expected error for empty input, got nil")
	}
}
