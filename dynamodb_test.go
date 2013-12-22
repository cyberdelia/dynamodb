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

func BenchmarkPut(b *testing.B) {
	if testing.Short() {
		b.Skip()
	}
	for i := 0; i < b.N; i++ {
		err := Put("papers", &paper{
			Title:   "Dynamo: Amazon’s Highly Available Key-value Store",
			Year:    2007,
			Score:   1.5,
			Authors: []string{"Giuseppe DeCandia", "Werner Vogels", "Deniz Hastorun"},
		})
		if err != nil {
			b.Fatal(err)
		}
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

func BenchmarkGet(b *testing.B) {
	if testing.Short() {
		b.Skip()
	}
	item := &paper{
		Title: "Dynamo: Amazon’s Highly Available Key-value Store",
		Year:  2007,
	}
	for i := 0; i < b.N; i++ {
		err := Get("papers", item)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestAll(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	items, err := All("papers", &paper{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items.([]*paper)) < 1 {
		t.Error("no items returned")
	}
}

func TestPluck(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	items, err := Pluck("papers", &paper{}, "year")
	if err != nil {
		t.Fatal(err)
	}
	papers := items.([]*paper)
	if len(papers) < 1 {
		t.Error("no items returned")
	}
	if papers[0].Title != "" {
		t.Error("returned title")
	}
	if papers[0].Year == 0 {
		t.Error("didn't return year")
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
