package dynamodb_test

import (
	"github.com/cyberdelia/dynamodb"
)

type Paper struct {
	Title   string   `dynamo:"title,hash"`
	Year    int      `dynamo:"year,range"`
	Score   float64  `dynamo:"score"`
	Authors []string `dynamo:"authors"`
}

func ExampleGet() {
	paper := &Paper{
		Title: "Dynamo: Amazon’s Highly Available Key-value Store",
		Year:  2007,
	}
	err := dynamodb.Get("papers", paper)
	if err != nil {
		// ...
	}
}

func ExamplePut() {
	err := dynamodb.Put("papers", &Paper{
		Title:   "Dynamo: Amazon’s Highly Available Key-value Store",
		Year:    2007,
		Score:   1.5,
		Authors: []string{"Giuseppe DeCandia", "Werner Vogels", "Deniz Hastorun"},
	})
	if err != nil {
		// ...
	}
}

func ExampleDelete() {
	paper := &Paper{
		Title: "Dynamo: Amazon’s Highly Available Key-value Store",
		Year:  2007,
	}
	err := dynamodb.Delete("papers", paper)
	if err != nil {
		// ...
	}
}

func ExampleDeleteTable() {
	err := dynamodb.DeleteTable("papers")
	if err != nil {
		// ...
	}
}
