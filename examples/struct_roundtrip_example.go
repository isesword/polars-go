package main

import (
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/isesword/polars-go-bridge/polars"
)

type Profile struct {
	City string `polars:"city"`
	Zip  int64  `polars:"zip"`
}

type Employee struct {
	ID      int64    `polars:"id"`
	Name    string   `polars:"name"`
	Age     *int     `polars:"age"`
	Tags    []string `polars:"tags"`
	Payload []byte   `polars:"payload"`
	Profile Profile  `polars:"profile"`
}

func main() {
	fmt.Println("=== Polars Go Bridge - Struct Round Trip Example ===")

	age := 35
	rows := []Employee{
		{
			ID:      1,
			Name:    "Alice",
			Tags:    []string{"go", "polars"},
			Payload: []byte("alpha"),
			Profile: Profile{City: "Shanghai", Zip: 200000},
		},
		{
			ID:      2,
			Name:    "Bob",
			Age:     &age,
			Tags:    []string{"arrow"},
			Payload: []byte("beta"),
			Profile: Profile{City: "Suzhou", Zip: 215000},
		},
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "profile", Type: arrow.StructOf(
			arrow.Field{Name: "city", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "zip", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
	}, nil)

	df, err := polars.NewDataFrame(rows, polars.WithArrowSchema(schema))
	if err != nil {
		log.Fatal(err)
	}
	defer df.Close()

	fmt.Println("1) DataFrame.Print()")
	if err := df.Print(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n2) 导出为 []Employee")
	typedRows, err := polars.ToStructs[Employee](df)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%#v\n", typedRows)

	fmt.Println("\n3) 导出为 []*Employee")
	ptrRows, err := polars.ToStructPointers[Employee](df)
	if err != nil {
		log.Fatal(err)
	}
	if len(ptrRows) > 0 && ptrRows[0] != nil {
		fmt.Printf("first pointer row: %#v\n", *ptrRows[0])
	}
}
