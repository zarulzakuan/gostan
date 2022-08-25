package gostan

import (
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/joho/godotenv"
)

var (
	accountName string
	accountKey  string
	blobURL     string
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	accountName = os.Getenv("ACCOUNT_NAME")
	accountKey = os.Getenv("ACCOUNT_KEY")
	blobURL = os.Getenv("BLOB_URL")
}

func TestReverseReadBlobIncludeHeader(t *testing.T) {
	control_text := `id,date,name
20,8/24/2022,Dagon
19,8/24/2022,Welfare
18,8/24/2022,Tunsley
17,8/24/2022,Jacmard
16,8/24/2022,Cutler
15,8/24/2022,Chaucer
14,8/24/2022,Bellord
13,8/24/2022,Darinton
12,8/24/2022,Limeburn
11,8/24/2022,Rainger
10,8/25/2022,Truelove
9,8/25/2022,Druery
8,8/25/2022,Laux
7,8/25/2022,Arghent
6,8/25/2022,Monketon
5,8/25/2022,Tabourin
4,8/25/2022,Dowty
3,8/25/2022,Canet
2,8/25/2022,Withur
1,8/25/2022,Schoenleiter
`
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Println(err.Error())
	}

	blobClient, err := azblob.NewBlockBlobClientWithSharedKey(blobURL, cred, nil)

	if err != nil {
		fmt.Println(err.Error())
	}
	r, w := io.Pipe()

	go ReverseReadBlob(w, blobClient, MAX_LENGTH, &ReadCondition{IncludeHeader: true})

	experiment_text := ""
	for {
		buff := make([]byte, MAX_LENGTH)
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
}

func TestReverseReadBlobRowLimit(t *testing.T) {
	control_text := `id,date,name
20,8/24/2022,Dagon
19,8/24/2022,Welfare
18,8/24/2022,Tunsley
17,8/24/2022,Jacmard
16,8/24/2022,Cutler
`
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Println(err.Error())
	}

	blobClient, err := azblob.NewBlockBlobClientWithSharedKey(blobURL, cred, nil)

	if err != nil {
		fmt.Println(err.Error())
	}
	r, w := io.Pipe()

	go ReverseReadBlob(w, blobClient, MAX_LENGTH, &ReadCondition{IncludeHeader: true, RowLimit: 5})

	experiment_text := ""
	for {
		buff := make([]byte, MAX_LENGTH)
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
}

func TestReverseReadBlobStopColDiff(t *testing.T) {
	control_text := `id,date,name
20,8/24/2022,Dagon
19,8/24/2022,Welfare
18,8/24/2022,Tunsley
17,8/24/2022,Jacmard
16,8/24/2022,Cutler
15,8/24/2022,Chaucer
14,8/24/2022,Bellord
13,8/24/2022,Darinton
12,8/24/2022,Limeburn
11,8/24/2022,Rainger
`
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Println(err.Error())
	}

	blobClient, err := azblob.NewBlockBlobClientWithSharedKey(blobURL, cred, nil)

	if err != nil {
		fmt.Println(err.Error())
	}
	r, w := io.Pipe()

	go ReverseReadBlob(w, blobClient, MAX_LENGTH, &ReadCondition{IncludeHeader: true, StopIfColValuesDiffer: ColumnNames{"Date"}})

	experiment_text := ""
	for {
		buff := make([]byte, MAX_LENGTH)
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
}

func TestReverseReadBlobStopRegexMatched(t *testing.T) {
	control_text := `id,date,name
20,8/24/2022,Dagon
19,8/24/2022,Welfare
18,8/24/2022,Tunsley
17,8/24/2022,Jacmard
16,8/24/2022,Cutler
15,8/24/2022,Chaucer
`
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Println(err.Error())
	}

	blobClient, err := azblob.NewBlockBlobClientWithSharedKey(blobURL, cred, nil)

	if err != nil {
		fmt.Println(err.Error())
	}
	r, w := io.Pipe()

	go ReverseReadBlob(w, blobClient, MAX_LENGTH, &ReadCondition{IncludeHeader: true, StopIfRegexMatched: regexp.MustCompile("Bellord")})

	experiment_text := ""
	for {
		buff := make([]byte, MAX_LENGTH)
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
}

func TestReverseReadBlobAllCondition(t *testing.T) {
	control_text := `id,date,name
20,8/24/2022,Dagon
19,8/24/2022,Welfare
18,8/24/2022,Tunsley
17,8/24/2022,Jacmard
16,8/24/2022,Cutler
15,8/24/2022,Chaucer
14,8/24/2022,Bellord
13,8/24/2022,Darinton
12,8/24/2022,Limeburn
11,8/24/2022,Rainger
`
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Println(err.Error())
	}

	blobClient, err := azblob.NewBlockBlobClientWithSharedKey(blobURL, cred, nil)

	if err != nil {
		fmt.Println(err.Error())
	}
	r, w := io.Pipe()

	cols := ColumnNames{"date"}
	re := regexp.MustCompile("Monketon")
	go ReverseReadBlob(w, blobClient, MAX_LENGTH, &ReadCondition{IncludeHeader: true, StopIfRegexMatched: re, StopIfColValuesDiffer: cols})

	experiment_text := ""
	for {
		buff := make([]byte, MAX_LENGTH)
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
}

func TestReverseReadBlobNoCondition(t *testing.T) {
	control_text := `20,8/24/2022,Dagon
19,8/24/2022,Welfare
18,8/24/2022,Tunsley
17,8/24/2022,Jacmard
16,8/24/2022,Cutler
15,8/24/2022,Chaucer
14,8/24/2022,Bellord
13,8/24/2022,Darinton
12,8/24/2022,Limeburn
11,8/24/2022,Rainger
10,8/25/2022,Truelove
9,8/25/2022,Druery
8,8/25/2022,Laux
7,8/25/2022,Arghent
6,8/25/2022,Monketon
5,8/25/2022,Tabourin
4,8/25/2022,Dowty
3,8/25/2022,Canet
2,8/25/2022,Withur
1,8/25/2022,Schoenleiter
id,date,name
`
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Println(err.Error())
	}

	blobClient, err := azblob.NewBlockBlobClientWithSharedKey(blobURL, cred, nil)

	if err != nil {
		fmt.Println(err.Error())
	}
	r, w := io.Pipe()

	go ReverseReadBlob(w, blobClient, MAX_LENGTH, &ReadCondition{})

	experiment_text := ""
	for {
		buff := make([]byte, MAX_LENGTH)
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
}
