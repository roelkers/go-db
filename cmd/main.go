package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
  "github.com/roelkers/go_db/pkg/table"
)

func main() {
	t := table.MakeTable()
  reader := bufio.NewReader(os.Stdin)
  fmt.Println("Please enter a database command")
  for {
    text, _ := reader.ReadString('\n');
    text = strings.TrimSuffix(text, "\n")
    if text[0] == '.' {
      err := t.DoMetaCommand(text) 
      if err != nil {
        fmt.Printf("Unrecognized command %s \n", text)
      } else {
        continue
      }
    }
    statement := new(table.Statement)
    err := t.PrepareStatement(text, statement) 
    if err != nil {
       fmt.Println(err)
       continue
    } 

    t.ExecuteStatement(statement)
    fmt.Println("Executed")
  }
}
