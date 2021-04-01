package dirqueue

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/tejainece/uu"
)

type DirQueue struct {
	RootDir   string
	TmpDir    string
	DataDir   string
	QueueDir  string
	ActiveDir string
}

type Options struct {
	Metadata map[string]string
	Priority uint8
}

type Job struct {
	ts          time.Time
	opts        *Options
	hostname    string
	size        int64
	pathtmpdata string
	pathdata    string
	pathtmpctrl string
}

// Regexen
var reDot = regexp.MustCompile(`\.`)
var reAlphanum = regexp.MustCompile(`[^A-Za-z0-9+_]`)
var reControlKeyFormat = regexp.MustCompile(`^Q...$`)
var reControlKeyBadChars = regexp.MustCompile("[:\000\n]")
var reControlValBadChars = regexp.MustCompile("[\000\n]")

func ensureDirExists(dir string) error {
	stat, err := os.Stat(dir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil && stat.IsDir() {
		return nil
	}
	return os.MkdirAll(dir, 0777)
}

func hashStringToFilename(s string) string {
	// # get a 16-bit checksum of the input, then uuencode that string
	// $str = pack ("u*", unpack ("%16C*", $str));
	sum := 0
	for i := 0; i < len(s); i++ {
		sum += int(s[i])
	}
	ustr := string(uu.EncodeLine([]byte(fmt.Sprintf("%d", sum))))

	// # transcode from uuencode-space into safe, base64-ish space
	// $str =~ y/ -_/A-Za-z0-9+_/;
	tstr := strings.Map(func(r rune) rune {
		if r >= ' ' && r <= ':' {
			r += 33
		} else if r > ':' && r <= 'T' {
			r += 39
		} else if r > 'T' && r <= ']' {
			r -= 36
		} else if r == '^' {
			r = '+'
		}
		return r
	}, ustr)

	// # and remove the stuff that wasn't in that "safe" range
	// $str =~ y/A-Za-z0-9+_//cd;
	return reAlphanum.ReplaceAllString(tstr, "")
}

func newJob(opts *Options) (Job, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return Job{}, err
	}
	return Job{ts: time.Now().UTC(), opts: opts, hostname: hostname}, nil
}

func (j Job) newQueueFilename(appendRandom bool) string {
	timestr := j.ts.Format("20060102030405.000000")
	qfname := fmt.Sprintf("%02d.%20s.%s",
		j.opts.Priority,
		reDot.ReplaceAllString(timestr, ""),
		hashStringToFilename(fmt.Sprintf("%s%d", j.hostname, os.Getpid())),
	)
	// This isn't normally needed, but if there's a collision
	// subsequent retries will set appendRandom to true to try
	// and avoid further collisions
	if appendRandom {
		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)
		qfname += fmt.Sprintf(".%d.%d", os.Getpid(), r.Intn(65536))
	}
	return qfname
}

// cleanup tries to remove any files we have created, ignoring errors
func (j Job) cleanup() {
	if j.pathtmpdata != "" {
		_ = os.Remove(j.pathtmpdata)
	}
	if j.pathdata != "" {
		_ = os.Remove(j.pathdata)
	}
	if j.pathtmpctrl != "" {
		_ = os.Remove(j.pathtmpctrl)
	}
}

func dqSubdir(rootdir string, subdir string) (string, error) {
	pathdir := filepath.Join(rootdir, subdir)
	err := ensureDirExists(pathdir)
	if err != nil {
		return "", err
	}
	return pathdir, nil
}

// hashqfname strips the last two alphanumeric characters from qfname
// and returns them with the stripped filename
func hashqfname(qfname string) (string, string, string) {
	lvl1 := "0"
	lvl2 := "0"
	reStrip := regexp.MustCompile(`^(.*)([A-Za-z0-9+_])([A-Za-z0-9+_])$`)
	matches := reStrip.FindStringSubmatch(qfname)
	if matches != nil {
		qfname = matches[1]
		lvl1 = matches[2]
		lvl2 = matches[3]
	}
	//fmt.Printf("+ hashing: %q => %q, %q, %q\n", qcname, qfname, lvl1, lvl2)
	return lvl1, lvl2, qfname
}

// createHashedDataDir strips the last two characters from qfname
// to create data hash directories in datadir
func createHashedDataDir(datadir, qfname string) (string, string, error) {
	lvl1, lvl2, qfname := hashqfname(qfname)

	// Create hashed data dir
	pathdatadir := filepath.Join(datadir, lvl1, lvl2)
	err := ensureDirExists(pathdatadir)
	if err != nil {
		return "", "", err
	}

	return pathdatadir, qfname, nil
}

// linkIntoDir tries to create a hard link to pathsrc in dstdir with
// filename qfname. On failure, it retries up to 10 times, with
// modified filenames with additional random characters appended.
// Returns the full path to the linked file on success.
func linkIntoDir(pathsrc, dstdir, qfname string, job Job) (string, error) {
	var path string
	maxRetries := 10

	for retry := 1; retry <= maxRetries; retry++ {
		path = filepath.Join(dstdir, qfname)

		err := os.Link(pathsrc, path)
		if err == nil {
			break
		}

		// Failed - check if we have hit maxRetries
		if retry == maxRetries {
			return "", fmt.Errorf("failed to link %q to %q", pathsrc, path)
		}

		// Try a new filename, with randomness added
		qfname = job.newQueueFilename(true)
		time.Sleep(time.Duration(retry) * 250 * time.Microsecond)
	}

	// Success - remove pathsrc since we don't need it any more
	err := os.Remove(pathsrc)
	if err != nil {
		// Warn, but non-fatal
		fmt.Fprintf(os.Stderr, "failed to remove hardlinked tmp file %q: %s",
			pathsrc, err.Error())
	}

	return path, nil
}

func createControlFile(pathtmpctrl string, job Job) error {
	fh, err := os.Create(pathtmpctrl)
	if err != nil {
		return err
	}

	pathdata, err := filepath.Abs(job.pathdata)
	if err != nil {
		return err
	}
	tsSeconds := job.ts.Unix()
	tsMicroseconds := int64(job.ts.UnixNano()/1000) - tsSeconds*1000000

	fmt.Fprintf(fh, "QDFN: %s\n", pathdata)
	fmt.Fprintf(fh, "QDSB: %d\n", job.size)
	fmt.Fprintf(fh, "QSTT: %d\n", tsSeconds)
	fmt.Fprintf(fh, "QSTM: %d\n", tsMicroseconds)
	fmt.Fprintf(fh, "QSHN: %s\n", job.hostname)

	// Check and store metadata
	for k, v := range job.opts.Metadata {
		// Check keys and values
		if reControlKeyFormat.MatchString(k) ||
			reControlKeyBadChars.MatchString(k) ||
			reControlValBadChars.MatchString(v) {
			_ = fh.Close()
			job.cleanup()
			return fmt.Errorf("invalid metadatum: %q", k)
		}
		fmt.Fprintf(fh, "%s: %s\n", k, v)
	}

	err = fh.Close()
	if err != nil {
		return err
	}

	return nil
}

// New returns a reference to a DirQueue struct for the
// queue in rootdir
func New(rootdir string) (*DirQueue, error) {
	err := ensureDirExists(rootdir)
	if err != nil {
		return nil, err
	}

	// These would be nice to do via a for loop, but there's no way to set
	// the struct members that way
	pathtmpdir, err := dqSubdir(rootdir, "tmp")
	if err != nil {
		return nil, err
	}
	pathdatadir, err := dqSubdir(rootdir, "data")
	if err != nil {
		return nil, err
	}
	pathqueuedir, err := dqSubdir(rootdir, "queue")
	if err != nil {
		return nil, err
	}
	pathactivedir, err := dqSubdir(rootdir, "active")
	if err != nil {
		return nil, err
	}

	return &DirQueue{
		RootDir:   rootdir,
		TmpDir:    pathtmpdir,
		DataDir:   pathdatadir,
		QueueDir:  pathqueuedir,
		ActiveDir: pathactivedir,
	}, nil
}

// DefaultOptions returns a reference to an Options struct
// with default member values
func DefaultOptions() *Options {
	return &Options{Metadata: map[string]string{}, Priority: 50}
}

// EnqueueReader enqueues the data in rdr into the current queue
// (with options in opts, if set).
// This is the equivalent to the perl IPC::DirQueue::enqueue_fh().
func (dq *DirQueue) EnqueueReader(rdr io.Reader, opts *Options) error {
	if opts == nil {
		opts = DefaultOptions()
	}
	if opts.Priority > 99 {
		opts.Priority = 99
	}

	job, err := newJob(opts)
	if err != nil {
		return err
	}
	qfname := job.newQueueFilename(false)
	//fmt.Printf("+ qfname: %s\n", qfname)
	qcname := qfname

	pathtmpctrl := filepath.Join(dq.TmpDir, qfname+".ctrl")
	pathtmpdata := filepath.Join(dq.TmpDir, qfname+".data")

	outfh, err := os.Create(pathtmpdata)
	if err != nil {
		return err
	}
	size, err := io.Copy(outfh, rdr)
	if err != nil {
		return err
	}
	job.size = size
	job.pathtmpdata = pathtmpdata

	// Create hashed datadir for qfname
	var pathdatadir string
	pathdatadir, qfname, err = createHashedDataDir(dq.DataDir, qfname)
	if err != nil {
		job.cleanup()
		return err
	}

	// Now link(2) the data tmpfile into pathdatadir
	pathdata, err := linkIntoDir(pathtmpdata, pathdatadir, qfname, job)
	if err != nil {
		job.cleanup()
		return err
	}
	job.pathdata = pathdata

	// Write a control file now that we know the actual data filename
	err = createControlFile(pathtmpctrl, job)
	if err != nil {
		job.cleanup()
		return err
	}
	job.pathtmpctrl = pathtmpctrl

	// And link(2) the control file into the queue directory
	_, err = linkIntoDir(pathtmpctrl, dq.QueueDir, qcname, job)
	if err != nil {
		job.cleanup()
		return err
	}

	// Touch dq.QueueDir to indicate it's been changed and a file has been enqueued
	// (required for some filesystems? e.g. XFS, ReiserFS)
	now := time.Now().UTC()
	err = os.Chtimes(dq.QueueDir, now, now)
	if err != nil {
		// IPC::DirQueue behaviour on failure is to warn, but continue
		fmt.Fprintf(os.Stderr, "touch failed on %q\n", dq.QueueDir)
	}

	return nil
}

// EnqueueFile enqueues the data file in path into the current queue
// (with options in opts, if set)
func (dq *DirQueue) EnqueueFile(path string, opts *Options) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	return dq.EnqueueReader(fh, opts)
}

// EnqueueString enqueues the data in string into the current queue
// (with options in opts, if set)
func (dq *DirQueue) EnqueueString(data string, opts *Options) error {
	rdr := strings.NewReader(data)
	return dq.EnqueueReader(rdr, opts)
}
