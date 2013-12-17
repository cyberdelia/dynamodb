package dynamodb

import (
	"testing"
)

type paper struct {
	Title   string   `dynamo:"title,hash"`
	Year    int      `dynamo:"year,range"`
	Score   float64  `dynamo:"score"`
	Authors []string `dynamo:"authors"`
}

// func TestCreateTable(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip()
// 	}
// 	err := CreateTable("papers", &paper{}, 1, 1)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// }

func TestListTables(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_, err := ListTables()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDescribeTable(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_, err := DescribeTable("papers")
	if err != nil {
		t.Fatal(err)
	}
}

func TestPut(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	err := Put("papers", &paper{
		Title:   "Dynamo: Amazon’s Highly Available Key-value Store",
		Year:    2007,
		Score:   1.5,
		Authors: []string{"Giuseppe DeCandia", "Werner Vogels", "Deniz Hastorun"},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGet(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	item := &paper{
		Title: "Dynamo: Amazon’s Highly Available Key-value Store",
		Year:  2007,
	}
	err := Get("papers", item)
	if err != nil {
		t.Fatal(err)
	}
	if item.Score != 1.5 {
		t.Error("year don't match")
	}
}

func TestDelete(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	err := Delete("papers", &paper{
		Title: "Dynamo: Amazon’s Highly Available Key-value Store",
		Year:  2007,
	})
	if err != nil {
		t.Fatal(err)
	}
}

// func TestDeleteTable(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip()
// 	}
// 	err := DeleteTable("papers")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// }
