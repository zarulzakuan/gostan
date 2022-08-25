// gostan, a Malaysian slang word, meaning "go astern" or go backward is a file reverse reader
// The idea was borrowed from the 'tac' unix command.
// In short, it sets the file read position to the end of file and start
// looking for line separator (default is newline char \n), fill the characters to buffer
// and output it as the pipe writer. So you need to create an io.Pipe() first and
// read the data stream from io.PipeReader
//
// Todo: Allow other char(s) than \n as line delimiter
package gostan

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// Default max buffer lenght is 8kb
const MAX_LENGTH int64 = 8192

type ColumnNames []string

type ReadCondition struct {
	StopIfColValuesDiffer ColumnNames
	StopIfRegexMatched    *regexp.Regexp
	StopIfRegexNotMatched *regexp.Regexp // todo
	RowLimit              int64
	RegexFilter           *regexp.Regexp // todo
	IncludeHeader         bool
}

func SetNewlineSeperator(sep []byte) {
	// TODO
}

// ReverseReadFiles reads local file(s) from EOF
func ReverseReadFiles(out *io.PipeWriter, readCondition *ReadCondition, file_descriptors ...*os.File) {
	delim_char := byte('\n')
	// get header if needed
	var headers [][]byte
	var compare string
	if readCondition.StopIfColValuesDiffer != nil || readCondition.IncludeHeader {
		headers = GetFileHeader(file_descriptors[0], []byte{','})
	}
	if readCondition.IncludeHeader {
		out.Write(bytes.Join(headers, []byte{','}))
		out.Write([]byte{delim_char})

	}
	var row_count int64 = 0
	defer out.Close()
	for i := range file_descriptors {
		filefile_descriptor := file_descriptors[len(file_descriptors)-1-i]
		defer filefile_descriptor.Close()

		tempBuffer := make([]byte, 0)   // for temp buffer, it has to be dynamic depends on the size of remainder chars to be carry forward to the next buffer window
		outputBuffer := make([]byte, 0) // use this buffer to store 1 row at a time to be piped out to the next processor

		// find file offset position from the beginning of the file
		file_size, _ := filefile_descriptor.Seek(0, io.SeekEnd) // or cursor position
		file_pos := file_size

		first_scan := true

		// start reading file a buffer at a time
		for {
			readBuffer := [MAX_LENGTH]byte{}
			readBufferLen := MAX_LENGTH
			file_pos -= readBufferLen // on file, go back readBufferLen at a time

			// read byte from current cursor (file_pos), this cursor moves back readBufferLen at a time
			filefile_descriptor.ReadAt(readBuffer[:], file_pos)

			// initial index always at the back of readbuffer
			var string_start_index int64 = readBufferLen - 1
			var string_end_index int64 = readBufferLen - 1
			// var string_next_end_index int64 = readBufferLen - 1

			var delim_index int64 = -1

			if file_pos < 0 {
				if file_pos < -readBufferLen {

					// to include header, we assume the header is the last output. so only show the last output if there's no header
					if !readCondition.IncludeHeader {
						out.Write(tempBuffer[0:])
					}
					break
				}
				filefile_descriptor.ReadAt(readBuffer[:readBufferLen+file_pos], 0)

			}

			// scan backward. Remember, we're working in a read buffer now!
			for i := range readBuffer {
				scanner_index := (int(readBufferLen) - 1) - i
				// skip if non char
				if readBuffer[scanner_index] == '\x00' {
					string_end_index--
					continue
				}
				// append line sep if first read doesnt have it
				if first_scan && readBuffer[scanner_index] != delim_char {
					tempBuffer = append(tempBuffer, delim_char)
				}
				first_scan = false

				//println("-->", string(readBuffer[scanner_index]))
				// found delim
				if readBuffer[scanner_index] == delim_char {
					delim_index = int64(scanner_index)
					string_start_index = delim_index + int64(1) //  \n[start from here][X][X][X]

					// get the string behind this delim if within our readbuffer
					if delim_index < readBufferLen-1 {
						outputBuffer = append(outputBuffer, readBuffer[string_start_index:string_end_index+1]...)
					}

					// check if we have anything in temp buffer
					if len(tempBuffer) > 0 {
						// append to our output buffer
						outputBuffer = append(outputBuffer, tempBuffer[0:]...)
						tempBuffer = tempBuffer[:0]
					}

					// CONDITION 1
					if readCondition.StopIfColValuesDiffer != nil {
						// if we havent store the mapped string, store it. Else, compare
						if len(compare) == 0 {
							mapped_string := stringToMap(string(outputBuffer[0:]), headers)
							for _, colName := range readCondition.StopIfColValuesDiffer {
								compare += mapped_string[colName].(string)
							}
						} else {
							mapped_string := stringToMap(string(outputBuffer[0:]), headers)
							compare2 := ""
							for _, colName := range readCondition.StopIfColValuesDiffer {
								compare2 += mapped_string[colName].(string)
							}
							if compare2 != compare {
								return
							}
						}

					}

					// CONDITION 2
					if readCondition.StopIfRegexMatched != nil && readCondition.StopIfRegexMatched.MatchString(string(outputBuffer[0:])) {
						return
					}

					// CONDITION 3
					if readCondition.RowLimit > 0 && readCondition.RowLimit == row_count {
						return
					}

					// write it out
					out.Write(outputBuffer[0:])

					outputBuffer = outputBuffer[:0]
					string_end_index = delim_index
					row_count++
				}

				// end of scan without finding anything
				if scanner_index == 0 {
					// save everything in temp buffer
					t := make([]byte, len(readBuffer[0:string_end_index+1]))
					copy(t, readBuffer[0:string_end_index+1])
					tempBuffer = append(t, tempBuffer...)
				}

			}
			// empty the read buffer
			readBuffer = [MAX_LENGTH]byte{}
		}
	}
}

// ReverseReadBlob reads file on Azure blob storage from EOF
func ReverseReadBlob(out *io.PipeWriter, blobClient *azblob.BlockBlobClient, bufferSize int64, readCondition *ReadCondition) {

	// check the file first see if it's not empty (or still empty)
	prop, _ := blobClient.GetProperties(context.Background(), nil)

	getSizeAttemptCount := 0
	for *prop.ContentLength <= 0 && getSizeAttemptCount > 6 {
		getSizeAttemptCount++
		fmt.Println("File size cannot be 0. Retry attempt:", getSizeAttemptCount)
		prop, _ = blobClient.GetProperties(context.Background(), nil)
		time.Sleep(5 * time.Second)
	}

	delim_char := byte('\n')
	// get header if needed
	var headers [][]byte
	var compare string
	if readCondition.StopIfColValuesDiffer != nil || readCondition.IncludeHeader {
		headers = GetBlobHeader(blobClient, []byte{','}, 1024)
	}
	if readCondition.IncludeHeader {
		out.Write(bytes.Join(headers, []byte{','}))
		out.Write([]byte{delim_char})

	}
	var row_count int64 = 0
	defer out.Close()

	var offset int64 = *prop.ContentLength - bufferSize
	var cnt int64 = bufferSize

	tempBuffer := make([]byte, 0)   // for temp buffer, it has to be dynamic depends on the size of remainder chars to be carry forward to the next buffer window
	outputBuffer := make([]byte, 0) // use this buffer to store 1 row at a time to be piped out to the next processor
	first_scan := true

	for {
		if offset < 0 {
			if offset < -bufferSize {
				if !readCondition.IncludeHeader {
					out.Write(tempBuffer[0:])
				}
				break
			}
		}
		t := make([]byte, bufferSize)
		err := blobClient.DownloadToBuffer(context.TODO(), offset, cnt, t, azblob.DownloadOptions{
			// Progress: func(bytesTransferred int64) {
			// 	// fmt.Printf("Read %d bytes.\n", bytesTransferred)
			// },
		})
		if err != nil {
			fmt.Println(err.Error())
		}

		// start read from buffer

		reader := bytes.NewReader(t)

		readBufferLen := bufferSize
		// initial index always at the back of readbuffer
		var string_start_index int64 = readBufferLen - 1
		var string_end_index int64 = readBufferLen - 1
		// var string_next_end_index int64 = readBufferLen - 1
		var delim_index int64 = -1
		for {
			readBuffer := make([]byte, bufferSize)
			_, err := reader.Read(readBuffer)
			if err != nil {
				break
			}

			for i := range readBuffer {
				scanner_index := (int(readBufferLen) - 1) - i
				// skip if non char
				if readBuffer[scanner_index] == '\x00' {
					string_end_index--
					continue
				}
				// append line sep if first read doesnt have it
				if first_scan && readBuffer[scanner_index] != delim_char {
					tempBuffer = append(tempBuffer, delim_char)
				}
				first_scan = false

				//println("-->", string(readBuffer[scanner_index]))
				// found delim
				if readBuffer[scanner_index] == delim_char {
					delim_index = int64(scanner_index)
					string_start_index = delim_index + int64(1) //  \n[start from here][X][X][X]

					// get the string behind this delim if within our readbuffer
					if delim_index < readBufferLen-1 {
						outputBuffer = append(outputBuffer, readBuffer[string_start_index:string_end_index+1]...)
					}

					// check if we have anything in temp buffer
					if len(tempBuffer) > 0 {
						// append to our output buffer
						outputBuffer = append(outputBuffer, tempBuffer[0:]...)
						tempBuffer = tempBuffer[:0]
					}
					// CONDITION 1
					if readCondition.StopIfColValuesDiffer != nil {
						// if we havent store the mapped string, store it. Else, compare
						if len(compare) == 0 {
							mapped_string := stringToMap(string(outputBuffer[0:]), headers)
							for _, colName := range readCondition.StopIfColValuesDiffer {
								if mapped_string[colName] != nil {
									compare += mapped_string[colName].(string)
								}
							}
						} else {
							mapped_string := stringToMap(string(outputBuffer[0:]), headers)
							compare2 := ""
							for _, colName := range readCondition.StopIfColValuesDiffer {
								compare2 += mapped_string[colName].(string)
							}
							if compare2 != compare {
								return
							}
						}

					}

					// CONDITION 2
					if readCondition.StopIfRegexMatched != nil && readCondition.StopIfRegexMatched.MatchString(string(outputBuffer[0:])) {
						return
					}

					// CONDITION 3
					if readCondition.RowLimit > 0 && readCondition.RowLimit == row_count {
						return
					}

					// write it out
					out.Write(outputBuffer[0:])

					outputBuffer = outputBuffer[:0]
					string_end_index = delim_index
					row_count++
				}

				// end of scan without finding anything
				if scanner_index == 0 {
					// save everything in temp buffer
					t := make([]byte, len(readBuffer[0:string_end_index+1]))
					copy(t, readBuffer[0:string_end_index+1])
					tempBuffer = append(t, tempBuffer...)
				}
			}
		}
		offset -= bufferSize
	}
}

// GetBlobHeader reads the first line of the Azure blob
func GetBlobHeader(blobClient *azblob.BlockBlobClient, delim []byte, bufferSize int64) [][]byte {

	var offset int64 = 0
	var cnt int64 = bufferSize

	tempBuffer := make([]byte, 0)   // for temp buffer, it has to be dynamic depends on the size of remainder chars to be carry forward to the next buffer window
	outputBuffer := make([]byte, 0) // use this buffer to store 1 row at a time to be piped out to the next processor

	for {
		t := make([]byte, bufferSize)
		err := blobClient.DownloadToBuffer(context.TODO(), offset, cnt, t, azblob.DownloadOptions{})
		if err != nil {
			fmt.Println(err.Error())
		}

		reader := bytes.NewReader(t)

		var delim_char byte = byte('\n')

		for {
			var delim_index int64 = -1
			readBuffer := make([]byte, bufferSize)
			_, err := reader.Read(readBuffer)
			if err != nil {
				break
			}

			for i := range readBuffer {
				scanner_index := i

				// found delim
				if readBuffer[scanner_index] == delim_char {
					delim_index = int64(scanner_index)
					// check if we have anything in temp buffer
					if len(tempBuffer) > 0 {
						// append to our output buffer
						outputBuffer = append(outputBuffer, tempBuffer[0:]...)
					}
					outputBuffer = append(outputBuffer, readBuffer[0:delim_index]...)
					return bytes.Split(outputBuffer[0:], delim)

				}

				// end of scan without finding anything
				if scanner_index == len(readBuffer)-1 {
					// save everything in temp buffer
					tempBuffer = append(tempBuffer, readBuffer[0:]...)
				}
			}

		}
		offset += bufferSize
	}

}

// GetFileHeader reads the first line of the file
func GetFileHeader(fd *os.File, delim []byte) [][]byte {
	scanner := bufio.NewScanner(fd)
	if scanner.Scan() {
		b := scanner.Bytes()
		return bytes.Split(b, delim)
	}
	return nil

}
