package gostan

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func TestReverseReadFiles(t *testing.T) {
	control_text := `10,8/25/2022,Truelove
9,8/25/2022,Druery
8,8/25/2022,Laux
7,8/25/2022,Arghent
6,8/25/2022,Monketon
5,8/25/2022,Tabourin
4,8/25/2022,Dowty
3,8/25/2022,Canet
2,8/25/2022,Withur
1,8/25/2022,Schoenleiter
10,8/24/2022,Dagon
9,8/24/2022,Welfare
8,8/24/2022,Tunsley
7,8/24/2022,Jacmard
6,8/24/2022,Cutler
5,8/24/2022,Chaucer
4,8/24/2022,Bellord
3,8/24/2022,Darinton
2,8/24/2022,Limeburn
1,8/24/2022,Rainger
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

	go ReverseReadFiles(w, &ReadCondition{IncludeHeader: false}, fd1, fd2)

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
