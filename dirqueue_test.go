package dirqueue

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHashStringToFilename(t *testing.T) {
	str := "vox12345"
	expect := "DNjA"
	got := hashStringToFilename(str)
	assert.Equal(t, expect, got)
}

func nukeTree(t *testing.T, dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		t.Errorf("nukeTree failed on %q", dir)
	}
}

func nukeQueue(t *testing.T, testq string) {
	nukeTree(t, filepath.Join(testq, "tmp"))
	nukeTree(t, filepath.Join(testq, "data"))
	nukeTree(t, filepath.Join(testq, "queue"))
}

func runQueueTests(t *testing.T, testq string, filesize, priority int,
	metadata map[string]string) {

	qfglob := fmt.Sprintf("%d.%d*", priority, time.Now().Year())

	// Check data file exists
	df, err := filepath.Glob(filepath.Join(testq, "data", "?", "?", qfglob))
	assert.Nil(t, err, "data file Glob")
	if assert.Equal(t, 1, len(df), "one data file found") {
		stat, err := os.Stat(df[0])
		assert.Nil(t, err, "data file Stat")
		assert.Equal(t, int64(filesize), stat.Size(), "data file size")
	}

	// Check control file exists
	cf, err := filepath.Glob(filepath.Join(testq, "queue", qfglob))
	assert.Nil(t, err, "control file Glob")
	if assert.Equal(t, 1, len(cf), "one control file found") {
		// Check control filename maps to data filename as expected
		if len(df) == 1 {
			dfelt := strings.Split(df[0], "/")
			dfmunged := filepath.Join(dfelt[2:]...)
			lvl1, lvl2, cfstripped := hashqfname(filepath.Base(cf[0]))
			cfmunged := filepath.Join(lvl1, lvl2, cfstripped)
			assert.Equal(t, dfmunged, cfmunged, "munged control filename == data filename")
		}
		// Check control file filename format
		cfname := filepath.Base(cf[0])
		cfelt := strings.Split(cfname, ".")
		assert.True(t, len(cfelt) == 3 || len(cfelt) == 5, "control file name elements")
		assert.Equal(t, fmt.Sprintf("%d", priority), cfelt[0], "control file name priority")
		assert.Equal(t, 20, len(cfelt[1]), "control file name timestamp length")

		// Check control file contents
		data, err := ioutil.ReadFile(cf[0])
		assert.Nil(t, err, "control file read")
		ctrldata := make(map[string]string)
		lines := strings.Split(string(bytes.TrimSpace(data)), "\n")
		assert.Equal(t, 5+len(metadata), len(lines), "control file linecount")
		for _, line := range lines {
			idx := strings.Index(line, ": ")
			if idx > -1 {
				ctrldata[line[:idx]] = line[idx+2:]
			}
		}
		dfpath, _ := filepath.Abs(df[0])
		assert.Equal(t, dfpath, ctrldata["QDFN"], "control file data filename")
		assert.Equal(t, fmt.Sprintf("%d", filesize), ctrldata["QDSB"], "control file data filesize")
		assert.True(t, ctrldata["QSTT"] != "", "control file QSTT")
		assert.True(t, ctrldata["QSTM"] != "", "control file QSTM")
		assert.True(t, ctrldata["QSHN"] != "", "control file QSHN")
		for k, v := range metadata {
			assert.Equal(t, v, ctrldata[k], "control metadata "+k)
		}
	}

	// Check tmp directory is empty
	tf, err := filepath.Glob(filepath.Join(testq, "tmp", "*"))
	assert.Nil(t, err, "tmp Glob")
	assert.Equal(t, 0, len(tf), "no tmp files found")
}

func TestEnqueueFile(t *testing.T) {
	testq := "testqueue"
	testfile := "testdata/test1.txt"
	filesize := 64
	priority := 50
	metadata := map[string]string{
		"foo": "abc",
		"bar": "12345",
	}

	nukeQueue(t, testq)

	dq, err := New(testq)
	assert.Nil(t, err, "constructor")

	opts := DefaultOptions()
	opts.Metadata = metadata
	opts.Priority = uint8(priority)
	err = dq.EnqueueFile(testfile, opts)
	assert.Nil(t, err, "EnqueueFile")

	runQueueTests(t, testq, filesize, priority, metadata)
}

func TestEnqueueReader(t *testing.T) {
	testq := "testqueue"
	testfile := "testdata/test1.txt"
	filesize := 64
	priority := 30
	metadata := map[string]string{}

	nukeQueue(t, testq)

	dq, err := New(testq)
	assert.Nil(t, err, "constructor")

	opts := DefaultOptions()
	opts.Metadata = metadata
	opts.Priority = uint8(priority)
	fh, err := os.Open(testfile)
	assert.Nil(t, err, "testfile open")
	defer fh.Close()
	err = dq.EnqueueReader(fh, opts)
	assert.Nil(t, err, "EnqueueReader")

	runQueueTests(t, testq, filesize, priority, metadata)
}

func TestEnqueueString(t *testing.T) {
	testq := "testqueue"
	data := "Once upon a time there lived a princess who felt\nno particular inclination to marry a prince.\n"
	filesize := 94
	priority := 50
	metadata := map[string]string{
		"name":   "Princess Aurora",
		"status": "single",
	}

	nukeQueue(t, testq)

	dq, err := New(testq)
	assert.Nil(t, err, "constructor")

	opts := DefaultOptions()
	opts.Metadata = metadata
	//opts.Priority = uint8(priority)
	err = dq.EnqueueString(data, opts)
	assert.Nil(t, err, "EnqueueString")

	runQueueTests(t, testq, filesize, priority, metadata)
}

func TestEnqueueStringEmpty(t *testing.T) {
	testq := "testqueue"
	data := ""
	filesize := 0
	priority := 50
	metadata := map[string]string{
		"uuid": "65fc1b26-a6bf-489c-a75c-6c86bd6afa29",
	}

	nukeQueue(t, testq)

	dq, err := New(testq)
	assert.Nil(t, err, "constructor")

	opts := DefaultOptions()
	opts.Metadata["uuid"] = metadata["uuid"]
	//opts.Priority = uint8(priority)
	err = dq.EnqueueString(data, opts)
	assert.Nil(t, err, "EnqueueString")

	runQueueTests(t, testq, filesize, priority, metadata)
}
