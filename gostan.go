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
	"io"
	"os"
)

// Default max buffer lenght is 8kb
const MAX_LENGTH int64 = 8192

// ReadFromEnd Move file descriptor current position to the end and start reading backward
func ReadFromEnd(fr *os.File, out *io.PipeWriter) {
	defer out.Close()
	var readBuffer [MAX_LENGTH]byte           // for read buffer we have to use array instead of slice as we need to read it from behind
	tempBuffer := make([]byte, 0, MAX_LENGTH) // for temp buffer, it has to be dynamic depends on the size of remainder chars to be carry forward to the next buffer window
	outputBuffer := make([]byte, 0)           // use this buffer to store 1 row at a time to be piped out to the next processor

	readBufferLen := MAX_LENGTH
	var firstRead bool = true

	// find file offset position from the beginning of the file
	file_size, _ := fr.Seek(0, io.SeekEnd) // or cursor position
	file_pos := file_size

	// start reading file a buffer at a time
	for {
		file_pos -= readBufferLen // on file, go back readBufferLen at a time

		if firstRead {
			// this means file is smaller than our read buffer,
			if file_pos < 0 {
				// so we need to make the readbuffer to the beginning of the file == 0
				file_pos = 0
			}
		}

		// if ahead of the starting of the file, break
		if file_pos < 0 {
			//write the remaining stuff in our temp buffer if any
			if len(tempBuffer) > 0 {
				out.Write(tempBuffer[0:])
			}
			// and that's the end of file read
			break
		}

		// read byte from current cursor (file_pos), this cursor moves back readBufferLen at a time
		fr.ReadAt(readBuffer[:], file_pos)

		// for first read, append newline at the rightmost of readbuffer, so need to offset by 1 to make room
		if firstRead {
			fr.ReadAt(readBuffer[:], file_pos+1)
			readBuffer[readBufferLen-1] = byte('\n')
			// reset cursor
			file_pos += 1
		}

		string_start_pos := 0
		string_end_pos := 0
		string_next_end_pos := 0

		// scan backward
		for i := range readBuffer {
			searcher_pos := int(readBufferLen) - 1 - i
			string_end_pos = string_next_end_pos

			// start of scan
			if searcher_pos == int(readBufferLen)-1 {
				string_end_pos = int(readBufferLen) // always at the length of buffer
				string_next_end_pos = string_end_pos
			}

			// found delim
			if readBuffer[searcher_pos] == '\n' {
				string_start_pos = searcher_pos + 1
				string_next_end_pos = string_start_pos
				if string_start_pos > int(readBufferLen)-1 {
					// ignore
				} else {
					if len(tempBuffer) > 0 {
						// clear the temp buff but keep the underlying array
						tempBuffer = tempBuffer[:0]

						outputBuffer = append(outputBuffer, readBuffer[string_start_pos:string_end_pos]...)
						outputBuffer = append(outputBuffer, tempBuffer[0:]...)

						out.Write(outputBuffer[0:])
					} else {
						outputBuffer = append(outputBuffer, readBuffer[string_start_pos:string_end_pos]...)
						out.Write(outputBuffer[0:])
					}
					// clear the output buffer for the next row
					outputBuffer = outputBuffer[:0]
				}
			}

			// end of scan
			if searcher_pos == 0 {
				string_start_pos = 0
				// write to tempbuffer first because this is a partial row
				tempBuffer = append(tempBuffer, readBuffer[string_start_pos:string_end_pos]...)
				//reset our cursors
				string_next_end_pos = 0
			}
			firstRead = false
		}
	}
}
