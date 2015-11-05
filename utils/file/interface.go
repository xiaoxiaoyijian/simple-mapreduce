package file

import (
	"bufio"
	"io"
	"io/ioutil"
	"os"
	"regexp"
)

func ReadLines(filename string) chan interface{} {
	ret := make(chan interface{}, 10)

	go func() {
		defer close(ret)

		f, err := os.Open(filename)
		if err != nil {
			println(err.Error())
			return
		}
		defer f.Close()

		br := bufio.NewReader(f)
		for {
			line, _, err := br.ReadLine()
			if err != nil {
				if err == io.EOF {
					break
				}

				println(err.Error())
				return
			}

			ret <- string(line)
		}
	}()

	return ret
}

func ReadDir(path, pattern string) chan interface{} {
	ret := make(chan interface{}, 10)

	go func() {
		defer close(ret)

		fd, err := os.Stat(path)
		if err != nil {
			println(err.Error())
			return
		}
		if fd.IsDir() {
			files, err := ioutil.ReadDir(path)
			if err != nil {
				println(err.Error())
				return
			}

			var r *regexp.Regexp
			if pattern != "" {
				r, _ = regexp.Compile(pattern)
			}
			for _, f := range files {
				if r != nil && !r.MatchString(f.Name()) {
					continue
				}

				ret <- path + "/" + f.Name()
			}
		} else {
			ret <- path
		}
	}()

	return ret
}
