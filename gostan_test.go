package gostan

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func TestReadFromEnd(t *testing.T) {
	filename := "./mockfile"
	fr, err := os.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("open file error:%s", err.Error()))
	}
	defer fr.Close()

	r, w := io.Pipe()

	go ReadFromEnd(fr, w)
	f, _ := os.Create("result")
	for {
		buff := make([]byte, 50)
		n, err := r.Read(buff)
		if n != 0 {
			fmt.Print(string(buff[:n]))
			f.Write(buff[:n])
		}
		if err != nil {
			break
		}
	}
}
