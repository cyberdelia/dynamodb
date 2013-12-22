package dynamodb_test

import (
	"fmt"
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

func ExampleAll() {
	items, err := dynamodb.All("papers", &Paper{})
	if err != nil {
		// ...
	}
	papers := items.([]*Paper)
	fmt.Println(papers)
}

func ExamplePluck() {
	items, err := dynamodb.Pluck("papers", &Paper{}, "title", "year")
	if err != nil {
		// ...
	}
	for _, paper := range items.([]*Paper) {
		fmt.Println(paper.Title)
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

func ExampleCreateTable() {
	err := dynamodb.CreateTable("papers", &Paper{}, 1, 1)
	if err != nil {
		// ...
	}
}

func ExampleListTables() {
	tables, err := dynamodb.ListTables()
	if err != nil {
		// ...
	}
	fmt.Println(tables)
}

func ExampleDeleteTable() {
	err := dynamodb.DeleteTable("papers")
	if err != nil {
		// ...
	}
}
