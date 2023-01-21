package godb

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
  reader := bufio.NewReader(os.Stdin)
  text, _ := reader.ReadString('\n');

  fmt.Println(text)
}
