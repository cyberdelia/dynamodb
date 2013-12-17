// An intelligible dynamodb client
//
// This package allows you to store almost any struct with dynamodb.
// Each exported struct field becomes a member of the object unless
// the field's tag is "-".
//
// Default attribute name is the struct field name but can be specified
// in the struct field's tag value. The "dynamo" key in
// the struct field's tag value is the attribute name,
// followed by an optional comma and options. Examples:
//
//   // Field is ignored by this package.
//   Field int `dynamo:"-"`
//
//   // Field appears in table as attribute "myName".
//   Field int `dynamo:"myName"`
//
//   // Field is considered as an hash or range key in table.
//   Field string `dynamo:",hash"`
//   Field int    `dynamo:",range"`
//
package dynamodb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	aws "github.com/bmizerany/aws4"
	"net/http"
)

var (
	DefaultService = &Service{
		Region:  "us-east-1",
		Version: "20120810",
		Client:  aws.DefaultClient,
	}
)

type Service struct {
	Region  string
	Version string
	Client  *aws.Client
}

func (s *Service) Do(action string, body interface{}, a interface{}) error {
	url := fmt.Sprintf("https://dynamodb.%s.amazonaws.com/", s.Region)

	b, err := json.Marshal(body)
	if err != nil {
		return err
	}

	r, err := http.NewRequest("POST", url, bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "application/x-amz-json-1.0")
	r.Header.Set("X-Amz-Target", fmt.Sprintf("DynamoDB_%s.%s", s.Version, action))

	resp, err := s.Client.Do(r)
	if err != nil {
		return err
	}

	if status := resp.StatusCode; status != 200 {
		var e struct {
			Message string
			Type    string
		}
		json.NewDecoder(resp.Body).Decode(&e)
		return errors.New(fmt.Sprintf("%s: %s", e.Type, e.Message))
	}

	if a == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(a)
}

// Get the corresponding item from the given table.
func (s *Service) Get(tablename string, item interface{}) error {
	keys, err := marshall(item, true)
	if err != nil {
		return err
	}
	body := struct {
		TableName string
		Key       attributeValue
	}{
		TableName: tablename,
		Key:       keys,
	}
	var resp struct {
		Item attributeValue
	}
	err = s.Do("GetItem", body, &resp)
	if err != nil {
		return err
	}
	return unmarshall(resp.Item, item)
}

// Get the corresponding item from the given table.
func Get(tablename string, item interface{}) error {
	return DefaultService.Get(tablename, item)
}

// Create or replace the item in the given table.
func (s *Service) Put(tablename string, item interface{}) error {
	values, err := marshall(item, false)
	if err != nil {
		return err
	}
	body := struct {
		TableName string
		Item      attributeValue
	}{
		TableName: tablename,
		Item:      values,
	}
	return s.Do("PutItem", body, nil)
}

// Create or replace the item in the given table.
func Put(tablename string, item interface{}) error {
	return DefaultService.Put(tablename, item)
}

// Deletes corresponding item in the given table.
func (s *Service) Delete(tablename string, item interface{}) error {
	keys, err := marshall(item, true)
	if err != nil {
		return nil
	}
	body := struct {
		TableName string
		Key       attributeValue
	}{
		TableName: tablename,
		Key:       keys,
	}
	return s.Do("DeleteItem", body, nil)
}

// Deletes corresponding item in the given table.
func Delete(tablename string, item interface{}) error {
	return DefaultService.Delete(tablename, item)
}

// Deletes the given table.
func (s *Service) DeleteTable(tablename string) error {
	body := struct {
		TableName string
	}{
		TableName: tablename,
	}
	return s.Do("DeleteTable", body, nil)
}

// Deletes the given table.
func DeleteTable(tablename string) error {
	return DefaultService.DeleteTable(tablename)
}
