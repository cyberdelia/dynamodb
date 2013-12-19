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
	"github.com/cyberdelia/dynamodb/types"
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
func (s *Service) Get(tableName string, item interface{}) error {
	keys, err := types.Marshal(item, true)
	if err != nil {
		return err
	}
	body := struct {
		TableName string
		Key       types.AttributeValue
	}{
		TableName: tableName,
		Key:       keys,
	}
	var resp struct {
		Item types.AttributeValue
	}
	err = s.Do("GetItem", body, &resp)
	if err != nil {
		return err
	}
	return types.Unmarshal(resp.Item, item)
}

// Get the corresponding item from the given table.
func Get(tableName string, item interface{}) error {
	return DefaultService.Get(tableName, item)
}

// Create or replace the item in the given table.
func (s *Service) Put(tableName string, item interface{}) error {
	values, err := types.Marshal(item, false)
	if err != nil {
		return err
	}
	body := struct {
		TableName string
		Item      types.AttributeValue
	}{
		TableName: tableName,
		Item:      values,
	}
	return s.Do("PutItem", body, nil)
}

// Create or replace the item in the given table.
func Put(tableName string, item interface{}) error {
	return DefaultService.Put(tableName, item)
}

// Deletes corresponding item in the given table.
func (s *Service) Delete(tableName string, item interface{}) error {
	keys, err := types.Marshal(item, true)
	if err != nil {
		return nil
	}
	body := struct {
		TableName string
		Key       types.AttributeValue
	}{
		TableName: tableName,
		Key:       keys,
	}
	return s.Do("DeleteItem", body, nil)
}

// Deletes corresponding item in the given table.
func Delete(tableName string, item interface{}) error {
	return DefaultService.Delete(tableName, item)
}

// Creates table corresponding to the given item.
func (s *Service) CreateTable(tableName string, item interface{}, read, write int) error {
	definitions, err := types.Definitions(item)
	if err != nil {
		return err
	}
	keys, err := types.Keys(item)
	if err != nil {
		return err
	}
	body := struct {
		TableName             string
		ProvisionedThroughput types.ProvisionedThroughput
		AttributeDefinitions  types.AttributeDefinitions
		KeySchema             types.KeySchema
	}{
		TableName: tableName,
		ProvisionedThroughput: types.ProvisionedThroughput{
			ReadCapacityUnits:  read,
			WriteCapacityUnits: write,
		},
		AttributeDefinitions: definitions,
		KeySchema:            keys,
	}
	return s.Do("CreateTable", body, nil)
}

// Creates table corresponding to the given item.
func CreateTable(tableName string, item interface{}, read, write int) error {
	return DefaultService.CreateTable(tableName, item, read, write)
}

// List existing tables.
func (s *Service) ListTables() ([]string, error) {
	var resp struct {
		TableNames []string
	}
	err := s.Do("ListTables", new(struct{}), &resp)
	if err != nil {
		return nil, err
	}
	return resp.TableNames, nil
}

// List existing tables.
func ListTables() ([]string, error) {
	return DefaultService.ListTables()
}

// Describe given table.
func (s *Service) DescribeTable(tableName string) (types.Table, error) {
	body := struct {
		TableName string
	}{
		TableName: tableName,
	}
	var resp struct {
		Table types.Table
	}
	err := s.Do("DescribeTable", body, &resp)
	if err != nil {
		return types.Table{}, err
	}
	return resp.Table, nil
}

// Describe given table.
func DescribeTable(tableName string) (types.Table, error) {
	return DefaultService.DescribeTable(tableName)
}

// Deletes the given table.
func (s *Service) DeleteTable(tableName string) error {
	body := struct {
		TableName string
	}{
		TableName: tableName,
	}
	return s.Do("DeleteTable", body, nil)
}

// Deletes the given table.
func DeleteTable(tableName string) error {
	return DefaultService.DeleteTable(tableName)
}
