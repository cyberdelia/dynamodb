package dynamodb

import (
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var (
	ErrValuePointer = errors.New("dynamodb: value is not a pointer")
	ErrValueStruct  = errors.New("dynamodb: value is not a struct")
)

var textMarshalerType = reflect.TypeOf(new(encoding.TextMarshaler)).Elem()
var textUnmarshalerType = reflect.TypeOf(new(encoding.TextUnmarshaler)).Elem()

type AttributeValue map[string]map[string]interface{}

// Marshall struct into AttributeValue.
func Marshal(v interface{}, keys bool) (AttributeValue, error) {
	s := reflect.Indirect(reflect.ValueOf(v))
	if s.Kind() != reflect.Struct {
		return nil, ErrValueStruct
	}
	t := s.Type()
	values := make(AttributeValue)
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		ft := t.Field(i)
		tag := ft.Tag.Get("dynamo")
		if ft.Anonymous || isEmptyValue(f) || tag == "-" {
			// Skip anonymous fields and empty field
			continue
		}
		name, options := parseTag(tag)
		if name == "" {
			name = ft.Name
		}
		if keys && !(options.Contains("hash") || options.Contains("range")) {
			// Ignore non-keys field if asking only for keys
			continue
		}
		marshaler := typeMarshaler(f.Type())
		k, v := marshaler(f)
		values[name] = map[string]interface{}{
			k: v,
		}
	}
	return values, nil
}

type marshalFunc func(v reflect.Value) (string, interface{})

func typeMarshaler(t reflect.Type) marshalFunc {
	if t.Implements(textMarshalerType) {
		return textMarshaler
	}
	switch t.Kind() {
	case reflect.Bool:
		return boolMarshaler
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intMarshaler
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uintMarshaler
	case reflect.Float32:
		return float32Marshaler
	case reflect.Float64:
		return float64Marshaler
	case reflect.String:
		return stringMarshaler
	case reflect.Slice:
		return newSliceMarshaler(t)
	case reflect.Array:
		return newArrayMarshaler(t)
	default:
		panic(fmt.Sprintf("dynamodb: %s type is not supported", t.Kind()))
	}
}

func boolMarshaler(v reflect.Value) (string, interface{}) {
	return "S", strconv.FormatBool(v.Bool())
}

func intMarshaler(v reflect.Value) (string, interface{}) {
	return "N", strconv.FormatInt(v.Int(), 10)
}

func uintMarshaler(v reflect.Value) (string, interface{}) {
	return "N", strconv.FormatUint(v.Uint(), 10)
}

func float32Marshaler(v reflect.Value) (string, interface{}) {
	return "N", strconv.FormatFloat(v.Float(), 'f', -1, 32)
}

func float64Marshaler(v reflect.Value) (string, interface{}) {
	return "N", strconv.FormatFloat(v.Float(), 'f', -1, 64)
}

func stringMarshaler(v reflect.Value) (string, interface{}) {
	return "S", v.String()
}

func byteMarshaler(v reflect.Value) (string, interface{}) {
	return "B", string(v.Bytes())
}

func newSliceMarshaler(t reflect.Type) marshalFunc {
	if t.Elem().Kind() == reflect.Uint8 {
		return byteMarshaler
	}
	return newArrayMarshaler(t)
}

func newArrayMarshaler(t reflect.Type) marshalFunc {
	marshaler := typeMarshaler(t.Elem())
	return func(v reflect.Value) (string, interface{}) {
		var array []string
		var kind string
		n := v.Len()
		for i := 0; i < n; i++ {
			k, e := marshaler(v.Index(i))
			array = append(array, e.(string))
			kind = k
		}
		return fmt.Sprintf("%sS", kind), array
	}
}

func textMarshaler(v reflect.Value) (string, interface{}) {
	m := v.Interface().(encoding.TextMarshaler)
	b, _ := m.MarshalText()
	return "S", string(b)
}

// Unmarshall AttributeValue into struct.
func Unmarshal(a AttributeValue, v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return ErrValuePointer
	}
	s := reflect.Indirect(rv)
	if s.Kind() != reflect.Struct {
		return ErrValueStruct
	}
	t := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		ft := t.Field(i)
		tag := ft.Tag.Get("dynamo")
		if !f.IsValid() || !f.CanSet() || ft.Anonymous || tag == "-" {
			// Ignore unusable fields
			continue
		}
		name, _ := parseTag(tag)
		if name == "" {
			name = ft.Name
		}
		values, present := a[name]
		if !present {
			// Field not present in attributes values
			continue
		}
		unmarshaler := typeUnmarshaler(f.Type())
		for _, v := range values {
			fv, err := unmarshaler(reflect.ValueOf(v))
			if err != nil {
				return err
			}
			fc := fv.Convert(f.Type())
			f.Set(fc)
		}
	}
	return nil
}

type unmarshalFunc func(v reflect.Value) (reflect.Value, error)

func typeUnmarshaler(t reflect.Type) unmarshalFunc {
	if reflect.PtrTo(t).Implements(textMarshalerType) {
		return newTextUnmarshaler(t)
	}
	switch t.Kind() {
	case reflect.Bool:
		return boolUnmarshaler
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intUnmarshaler
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uintUnmarshaler
	case reflect.Float32, reflect.Float64:
		return floatUnmarshaler
	case reflect.String:
		return stringUnmarshaler
	case reflect.Slice:
		return newSliceUnmarshaler(t)
	case reflect.Array:
		return newArrayUnmarshaler(t)
	default:
		panic(fmt.Sprintf("dynamodb: %s type is not supported", t.Kind()))
	}
}

func boolUnmarshaler(v reflect.Value) (reflect.Value, error) {
	b, err := strconv.ParseBool(v.String())
	return reflect.ValueOf(b), err
}

func intUnmarshaler(v reflect.Value) (reflect.Value, error) {
	i, err := strconv.ParseInt(v.String(), 0, 64)
	return reflect.ValueOf(i), err
}

func uintUnmarshaler(v reflect.Value) (reflect.Value, error) {
	u, err := strconv.ParseUint(v.String(), 10, 64)
	return reflect.ValueOf(u), err
}

func floatUnmarshaler(v reflect.Value) (reflect.Value, error) {
	f, err := strconv.ParseFloat(v.String(), 64)
	return reflect.ValueOf(f), err
}

func stringUnmarshaler(v reflect.Value) (reflect.Value, error) {
	s := v.String()
	return reflect.ValueOf(s), nil
}

func byteUnmarshaler(v reflect.Value) (reflect.Value, error) {
	b := []byte(v.String())
	return reflect.ValueOf(b), nil
}

func newSliceUnmarshaler(t reflect.Type) unmarshalFunc {
	if t.Elem().Kind() == reflect.Uint8 {
		return byteUnmarshaler
	}
	return newArrayUnmarshaler(t)
}

func newArrayUnmarshaler(t reflect.Type) unmarshalFunc {
	ft := t.Elem()
	st := reflect.SliceOf(ft)
	unmarshaler := typeUnmarshaler(ft)
	return func(v reflect.Value) (reflect.Value, error) {
		n := v.Len()
		s := reflect.MakeSlice(st, 0, 0)
		for i := 0; i < n; i++ {
			value, err := unmarshaler(v.Index(i))
			if err != nil {
				return s, err
			}
			s = reflect.Append(s, value.Convert(ft))
		}
		return s, nil
	}
}

func newTextUnmarshaler(t reflect.Type) unmarshalFunc {
	n := reflect.New(t).Interface().(encoding.TextUnmarshaler)
	return func(v reflect.Value) (reflect.Value, error) {
		err := n.UnmarshalText([]byte(v.String()))
		ptr := reflect.ValueOf(n)
		return reflect.Indirect(ptr), err
	}
}

var timeType = reflect.TypeOf(time.Time{})

func isEmptyValue(v reflect.Value) bool {
	if v.Type() == timeType {
		t := v.Interface().(time.Time)
		return t.IsZero()
	}
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

type options string

func parseTag(tag string) (string, options) {
	if i := strings.Index(tag, ","); i != -1 {
		return tag[:i], options(tag[i+1:])
	}
	return tag, options("")
}

func (o options) Contains(name string) bool {
	if len(o) == 0 {
		return false
	}
	s := string(o)
	for s != "" {
		var next string
		i := strings.Index(s, ",")
		if i >= 0 {
			s, next = s[:i], s[i+1:]
		}
		if s == name {
			return true
		}
		s = next
	}
	return false
}
