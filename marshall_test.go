package dynamodb

import (
	"reflect"
	"testing"
)

type structTest struct {
	Int         int
	IntArray    []int
	Float       float32
	FloatArray  []float64
	String      string
	StringArray []string
	Blob        []byte
	BlobArray   [][]byte
}

var marshallTests = []struct {
	item  structTest
	value attributeValue
}{
	{
		structTest{
			Int:      8,
			IntArray: []int{8, 12},
		},
		attributeValue{
			"Int": {
				"N": "8",
			},
			"IntArray": {
				"NS": []string{"8", "12"},
			},
		},
	},
	{
		structTest{
			Float:      8.12,
			FloatArray: []float64{8.12, 12.8},
		},
		attributeValue{
			"Float": {
				"N": "8.12",
			},
			"FloatArray": {
				"NS": []string{"8.12", "12.8"},
			},
		},
	},
	{
		structTest{
			String:      "abc",
			StringArray: []string{"a", "b"},
		},
		attributeValue{
			"String": {
				"S": "abc",
			},
			"StringArray": {
				"SS": []string{"a", "b"},
			},
		},
	},
	{
		structTest{
			Blob: []byte{'a', 'b', 'c'},
			BlobArray: [][]byte{
				{'a', 'b', 'c'}, {'d', 'e', 'f'},
			},
		},
		attributeValue{
			"Blob": {
				"B": "abc",
			},
			"BlobArray": {
				"BS": []string{"abc", "def"},
			},
		},
	},
}

func TestMarshall(t *testing.T) {
	for _, e := range marshallTests {
		v, err := marshall(e.item, false)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(v, e.value) {
			t.Errorf("got %v wants %v", v, e.value)
		}
	}
}

func TestUnmarshall(t *testing.T) {
	for _, e := range marshallTests {
		var item structTest
		err := unmarshall(e.value, &item)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(item, e.item) {
			t.Errorf("got %v wants %v", item, e.item)
		}
	}
}
