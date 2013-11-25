package dynamodb

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

var (
	ErrValuePointer = errors.New("dynamodb: value is not a pointer")
	ErrValueStruct  = errors.New("dynamodb: value is not a struct")
)

type attributeValue map[string]map[string]interface{}

func marshall(v interface{}, keys bool) (attributeValue, error) {
	s := reflect.Indirect(reflect.ValueOf(v))
	if s.Kind() != reflect.Struct {
		return nil, ErrValueStruct
	}
	t := s.Type()
	values := make(attributeValue)
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
		marshaller := typeMarshaller(f.Type())
		k, v := marshaller(f)
		values[name] = map[string]interface{}{
			k: v,
		}
	}
	return values, nil
}

type marshallFunc func(v reflect.Value) (string, interface{})

func typeMarshaller(t reflect.Type) marshallFunc {
	switch t.Kind() {
	case reflect.Bool:
		return boolMarshaller
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intMarshaller
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uintMarshaller
	case reflect.Float32:
		return float32Marshaller
	case reflect.Float64:
		return float64Marshaller
	case reflect.String:
		return stringMarshaller
	case reflect.Slice:
		return newSliceMarshaller(t)
	case reflect.Array:
		return newArrayMarshaller(t)
	default:
		panic(fmt.Sprintf("dynamodb: %s type is not supported", t.Kind()))
	}
}

func boolMarshaller(v reflect.Value) (string, interface{}) {
	return "S", strconv.FormatBool(v.Bool())
}

func intMarshaller(v reflect.Value) (string, interface{}) {
	return "N", strconv.FormatInt(v.Int(), 10)
}

func uintMarshaller(v reflect.Value) (string, interface{}) {
	return "N", strconv.FormatUint(v.Uint(), 10)
}

func float32Marshaller(v reflect.Value) (string, interface{}) {
	return "N", strconv.FormatFloat(v.Float(), 'f', -1, 32)
}

func float64Marshaller(v reflect.Value) (string, interface{}) {
	return "N", strconv.FormatFloat(v.Float(), 'f', -1, 64)
}

func stringMarshaller(v reflect.Value) (string, interface{}) {
	return "S", v.String()
}

func byteMarshaller(v reflect.Value) (string, interface{}) {
	return "B", string(v.Bytes())
}

func newSliceMarshaller(t reflect.Type) marshallFunc {
	if t.Elem().Kind() == reflect.Uint8 {
		return byteMarshaller
	}
	return newArrayMarshaller(t)
}

func newArrayMarshaller(t reflect.Type) marshallFunc {
	marshaller := typeMarshaller(t.Elem())
	return func(v reflect.Value) (string, interface{}) {
		var array []string
		var kind string
		n := v.Len()
		for i := 0; i < n; i++ {
			k, e := marshaller(v.Index(i))
			array = append(array, e.(string))
			kind = k
		}
		return fmt.Sprintf("%sS", kind), array
	}
}

func unmarshall(a attributeValue, v interface{}) error {
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
		unmarshaller := typeUnmarshaller(f.Type())
		for _, v := range values {
			fv, err := unmarshaller(reflect.ValueOf(v))
			if err != nil {
				return err
			}
			fc := fv.Convert(f.Type())
			f.Set(fc)
		}
	}
	return nil
}

type unmarshallFunc func(v reflect.Value) (reflect.Value, error)

func typeUnmarshaller(t reflect.Type) unmarshallFunc {
	switch t.Kind() {
	case reflect.Bool:
		return boolUnmarshaller
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intUnmarshaller
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uintUnmarshaller
	case reflect.Float32, reflect.Float64:
		return floatUnmarshaller
	case reflect.String:
		return stringUnmarshaller
	case reflect.Slice:
		return newSliceUnmarshaller(t)
	case reflect.Array:
		return newArrayUnmarshaller(t)
	default:
		panic(fmt.Sprintf("dynamodb: %s type is not supported", t.Kind()))
	}
}

func boolUnmarshaller(v reflect.Value) (reflect.Value, error) {
	b, err := strconv.ParseBool(v.String())
	return reflect.ValueOf(b), err
}

func intUnmarshaller(v reflect.Value) (reflect.Value, error) {
	i, err := strconv.ParseInt(v.String(), 0, 64)
	return reflect.ValueOf(i), err
}

func uintUnmarshaller(v reflect.Value) (reflect.Value, error) {
	u, err := strconv.ParseUint(v.String(), 10, 64)
	return reflect.ValueOf(u), err
}

func floatUnmarshaller(v reflect.Value) (reflect.Value, error) {
	f, err := strconv.ParseFloat(v.String(), 64)
	return reflect.ValueOf(f), err
}

func stringUnmarshaller(v reflect.Value) (reflect.Value, error) {
	s := v.String()
	return reflect.ValueOf(s), nil
}

func byteUnmarshaller(v reflect.Value) (reflect.Value, error) {
	b := []byte(v.String())
	return reflect.ValueOf(b), nil
}

func newSliceUnmarshaller(t reflect.Type) unmarshallFunc {
	if t.Elem().Kind() == reflect.Uint8 {
		return byteUnmarshaller
	}
	return newArrayUnmarshaller(t)
}

func newArrayUnmarshaller(t reflect.Type) unmarshallFunc {
	ft := t.Elem()
	st := reflect.SliceOf(ft)
	unmarshaller := typeUnmarshaller(ft)
	return func(v reflect.Value) (reflect.Value, error) {
		n := v.Len()
		s := reflect.MakeSlice(st, 0, 0)
		for i := 0; i < n; i++ {
			value, err := unmarshaller(v.Index(i))
			if err != nil {
				return s, err
			}
			s = reflect.Append(s, value.Convert(ft))
		}
		return s, nil
	}
}

func isEmptyValue(v reflect.Value) bool {
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
