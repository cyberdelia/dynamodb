package types

import (
	"reflect"
	"testing"
)

type schema struct {
	Hash  string `dynamo:"h,hash"`
	Range int    `dynamo:"r,range"`
}

func TestKeySchema(t *testing.T) {
	s, err := Keys(&schema{})
	if err != nil {
		t.Fatal(err)
	}
	control := KeySchema{
		KeySchemaElement{
			AttributeName: "h",
			KeyType:       "HASH",
		},
		KeySchemaElement{
			AttributeName: "r",
			KeyType:       "RANGE",
		},
	}
	if !reflect.DeepEqual(s, control) {
		t.Errorf("got %v wants %v", s, control)
	}
}

func TestAttributeDefinitions(t *testing.T) {
	d, err := Definitions(&schema{})
	if err != nil {
		t.Fatal(err)
	}
	control := AttributeDefinitions{
		AttributeDefinition{
			AttributeName: "h",
			AttributeType: "S",
		},
		AttributeDefinition{
			AttributeName: "r",
			AttributeType: "N",
		},
	}
	if !reflect.DeepEqual(d, control) {
		t.Errorf("got %v wants %v", d, control)
	}
}
