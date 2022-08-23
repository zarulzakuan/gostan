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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// Default max buffer lenght is 8kb
const MAX_LENGTH int64 = 8192

func SetNewlineSeperator(sep []byte) {
	// TODO
}

// ReadFromEnd Move file descriptor current position to the end and start reading backward
func ReverseReadFiles(out *io.PipeWriter, file_descriptors ...*os.File) {
	delim_char := byte('\n')
	defer out.Close()
	for i, _ := range file_descriptors {
		filefile_descriptor := file_descriptors[len(file_descriptors)-1-i]

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
					out.Write(tempBuffer[0:])
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
					out.Write(outputBuffer[0:])
					outputBuffer = outputBuffer[:0]
					string_end_index = delim_index
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

func ReverseReadBlob(out *io.PipeWriter, blobClient *azblob.BlockBlobClient) {

	prop, _ := blobClient.GetProperties(context.Background(), nil)

	var offset int64 = *prop.ContentLength - MAX_LENGTH
	var cnt int64 = MAX_LENGTH

	tempBuffer := make([]byte, 0)   // for temp buffer, it has to be dynamic depends on the size of remainder chars to be carry forward to the next buffer window
	outputBuffer := make([]byte, 0) // use this buffer to store 1 row at a time to be piped out to the next processor
	first_scan := true

	var delim_char byte = byte('\n')

	for {
		if offset < 0 {
			if offset < -MAX_LENGTH {
				out.Write(tempBuffer[0:])
				break
			}
		}
		t := make([]byte, MAX_LENGTH)
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

		readBufferLen := MAX_LENGTH
		// initial index always at the back of readbuffer
		var string_start_index int64 = readBufferLen - 1
		var string_end_index int64 = readBufferLen - 1
		// var string_next_end_index int64 = readBufferLen - 1
		var delim_index int64 = -1
		for {
			readBuffer := make([]byte, MAX_LENGTH)
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
					out.Write(outputBuffer[0:])

					outputBuffer = outputBuffer[:0]
					string_end_index = delim_index
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
		offset -= MAX_LENGTH
	}
}
