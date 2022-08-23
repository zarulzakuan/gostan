package gostan

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func TestReadFromEnd(t *testing.T) {
	control_text := `===== Second file =====
Fifth line - this is the last line
Fourth line -gibberish gibberish gibberish gibberish 
Third line -gibberish 
Second line - gibberish gibberish 
First line - this is the first line
===== Second file =====
===== First file =====
Fifth line - this is the last line
Fourth line -gibberish gibberish gibberish gibberish 
Third line -gibberish 
Second line - gibberish gibberish 
First line - this is the first line
===== First file =====
`
	filename1 := "./mockfile1"
	fd1, err := os.Open(filename1)
	if err != nil {
		panic(fmt.Sprintf("open file error:%s", err.Error()))
	}
	defer fd1.Close()

	filename2 := "./mockfile2"
	fd2, err := os.Open(filename2)
	if err != nil {
		panic(fmt.Sprintf("open file error:%s", err.Error()))
	}
	defer fd2.Close()

	r, w := io.Pipe()

	go ReverseReadFiles(w, fd1, fd2)

	experiment_text := ""
	for {
		buff := make([]byte, 50)
		n, err := r.Read(buff)
		if n != 0 {
			fmt.Print(string(buff[:n]))
			experiment_text += string(buff[:n])
		}
		if err != nil {
			break
		}
	}
	if experiment_text != control_text {
		t.Fail()
	}
	// println(result)
}
