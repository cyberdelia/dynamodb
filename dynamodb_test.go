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

func TestPut(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
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
		t.Skip("skipping test in short mode.")
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
		t.Skip("skipping test in short mode.")
	}
	err := Delete("papers", &paper{
		Title: "Dynamo: Amazon’s Highly Available Key-value Store",
		Year:  2007,
	})
	if err != nil {
		t.Fatal(err)
	}
}
