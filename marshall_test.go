package dynamodb

import (
	"reflect"
	"testing"
	"time"
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
	Time        time.Time
}

var marshallTests = []struct {
	item  structTest
	value AttributeValue
}{
	{
		structTest{
			Int:      8,
			IntArray: []int{8, 12},
		},
		AttributeValue{
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
		AttributeValue{
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
		AttributeValue{
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
		AttributeValue{
			"Blob": {
				"B": "abc",
			},
			"BlobArray": {
				"BS": []string{"abc", "def"},
			},
		},
	},
	{
		structTest{
			Time: time.Date(2013, 12, 12, 17, 55, 30, 0, time.UTC),
		},
		AttributeValue{
			"Time": {
				"S": "2013-12-12T17:55:30Z",
			},
		},
	},
}

func TestMarshall(t *testing.T) {
	for _, e := range marshallTests {
		v, err := Marshal(e.item, false)
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
		err := Unmarshal(e.value, &item)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(item, e.item) {
			t.Errorf("got %v wants %v", item, e.item)
		}
	}
}
