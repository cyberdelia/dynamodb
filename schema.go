package dynamodb

import (
	"fmt"
	"reflect"
)

type AttributeDefinitions []AttributeDefinition

type AttributeDefinition struct {
	AttributeName string
	AttributeType string
}

type KeySchema []KeySchemaElement

type KeySchemaElement struct {
	AttributeName string
	KeyType       string
}

type ProvisionedThroughput struct {
	ReadCapacityUnits  int
	WriteCapacityUnits int
}

func attributeDefinitions(v interface{}) (d AttributeDefinitions, err error) {
	s := reflect.Indirect(reflect.ValueOf(v))
	if s.Kind() != reflect.Struct {
		return nil, ErrValueStruct
	}
	t := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		ft := t.Field(i)
		tag := ft.Tag.Get("dynamo")
		name, options := parseTag(tag)
		if name == "" {
			name = ft.Name
		}
		if !(options.Contains("hash") || options.Contains("range")) {
			// Ignore non-keys field if asking only for keys
			continue
		}
		d = append(d, AttributeDefinition{
			AttributeName: name,
			AttributeType: typeGuess(f.Type()),
		})
	}
	return d, nil
}

func typeGuess(t reflect.Type) string {
	if t.Implements(textMarshalerType) {
		return "S"
	}
	switch t.Kind() {
	case reflect.Bool:
		return "S"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "N"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return "N"
	case reflect.Float32:
		return "N"
	case reflect.Float64:
		return "N"
	case reflect.String:
		return "S"
	default:
		panic(fmt.Sprintf("dynamodb: %s type is not valid key type", t.Kind()))
	}
}

func keySchema(v interface{}) (k KeySchema, err error) {
	s := reflect.Indirect(reflect.ValueOf(v))
	if s.Kind() != reflect.Struct {
		return nil, ErrValueStruct
	}
	t := s.Type()
	for i := 0; i < s.NumField(); i++ {
		ft := t.Field(i)
		tag := ft.Tag.Get("dynamo")
		name, options := parseTag(tag)
		if name == "" {
			name = ft.Name
		}
		if !(options.Contains("hash") || options.Contains("range")) {
			// Ignore non-keys field if asking only for keys
			continue
		}
		var key string
		if options.Contains("hash") {
			key = "HASH"
		}
		if options.Contains("range") {
			key = "RANGE"
		}
		k = append(k, KeySchemaElement{
			AttributeName: name,
			KeyType:       key,
		})
	}
	return k, nil
}
