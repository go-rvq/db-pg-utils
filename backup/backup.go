package backup

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-rvq/rvq/web"
	"github.com/go-rvq/rvq/x/i18n"
	"github.com/go-rvq/rvq/x/packages/db-tools"
	tarfs "github.com/nlepage/go-tarfs"
	"gorm.io/gorm"
)

type File struct {
	DbName    string
	Path      string
	Info      os.FileInfo
	CreatedAt time.Time
	Auto      bool
	Size      int64
}

func (f *File) Read() (b *Backup, err error) {
	if f.CreatedAt.IsZero() {
		name := strings.TrimSuffix(filepath.Base(f.Path), ".tar")
		if f.CreatedAt, err = time.Parse(FileNameTimeFormat, name); err != nil {
			return
		}
	}

	if f.Size == 0 {
		if f.Info != nil {
			f.Size = f.Info.Size()
		} else {
			var of *os.File

			if of, err = os.Open(f.Path); err != nil {
				return
			}

			defer of.Close()

			if f.Size, err = of.Seek(0, io.SeekEnd); err != nil {
				return
			}
			if _, err = of.Seek(0, io.SeekStart); err != nil {
				return
			}
		}
	}

	b = &Backup{
		pth: f.Path,
		Backup: db_tools.Backup{
			CreatedAt: f.CreatedAt,
			DbName:    f.DbName,
			Auto:      f.Auto,
		},
	}

	var FS fs.FS
	if FS, err = b.FS(); err != nil {
		return
	}

	var msg []byte
	if msg, err = fs.ReadFile(FS, "message.txt"); err != nil {
		return
	}

	var bf os.FileInfo
	if bf, err = fs.Stat(FS, "backup.pg"); err != nil {
		return
	}

	b.Size = bf.Size()
	b.Message = string(msg)

	b.OpenFunc = func() (r io.ReadCloser, err error) {
		var FS fs.FS
		if FS, err = b.FS(); err != nil {
			return
		}
		r, err = FS.Open("backup.pg")
		return
	}

	return
}

type Backup struct {
	db_tools.Backup
	pth string
}

func (b *Backup) OpenArchive() (io.ReadCloser, error) {
	return os.Open(b.pth)
}

func (b *Backup) FS() (fs fs.FS, err error) {
	var f *os.File
	if f, err = os.Open(b.pth); err != nil {
		return
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	fs, err = tarfs.New(f)
	return
}

type HeaderCb struct {
	Name string
	Cb   func(r io.ReadCloser, h *tar.Header)
}

func (b *Backup) FileReaders(name string) (hdr *tar.Header, r io.ReadCloser, err error) {
	type tfr struct {
		io.Closer
		io.Reader
	}

	var f *os.File

	if f, err = os.Open(b.pth); err != nil {
		return
	}

	defer func() {
		if err != nil {
			if f != nil {
				f.Close()
			}
		}
	}()

	tr := tar.NewReader(f)

	for {
		if hdr, err = tr.Next(); err == io.EOF {
			break
		}

		if err != nil {
			return
		}

		if hdr.Name == name {
			r = tfr{
				Closer: f,
				Reader: io.LimitReader(tr, hdr.Size),
			}
			return
		}
	}

	err = &os.PathError{
		Path: fmt.Sprintf("%s => %s", b.pth, name),
		Err:  os.ErrNotExist,
		Op:   "read",
	}

	return
}

func (b *Backup) String() string {
	var a = "MANUL"
	if b.Auto {
		a = "AUTO"
	}
	return fmt.Sprintf("%s (%s, %s, %s) => %q", b.DbName, b.CreatedAt.UTC(), a, b.Message)
}

func (b *Backup) DetailString(ctx context.Context) string {
	return GetMessages(ctx).BackupDetail(b.DbName, i18n.GetMessages(ctx).DateTimeFormatter().Full()(ctx)(b.CreatedAt), b.Message)
}

const FileNameTimeFormat = "20060102T150405"

var errListStop = errors.New("stop")

type Controller struct {
	db       *gorm.DB
	dir      string
	localDir string
	port     string
	log      *slog.Logger
}

var _ db_tools.BackupController = (*Controller)(nil)

func (b *Controller) getDb() *gorm.DB {
	return b.db.Session(&gorm.Session{})
}

func (c *Controller) ParseCreatedTime(v string) (time.Time, error) {
	return time.Parse(FileNameTimeFormat, v)
}

func (b *Controller) GetAutoLocalDir(auto bool) string {
	var sub = "manual"
	if auto {
		sub = "auto"
	}
	return filepath.Join(b.localDir, sub)
}

func (c *Controller) CurrentName(context.Context) (name string, err error) {
	err = c.getDb().Raw("SELECT current_database()").Pluck("name", &name).Error
	return
}

func (c *Controller) Download(w http.ResponseWriter, r *http.Request, id db_tools.BackupID) (err error) {
	var f *File
	if f, err = c.FileOf(id); err != nil {
		return
	}

	var osFile *os.File
	if osFile, err = os.Open(f.Path); err != nil {
		return
	}
	defer osFile.Close()

	h := w.Header()
	h.Set("Content-Disposition", "attachment; filename=\""+filepath.Base(f.Path)+"\"")
	h.Set("Cache-Control", "private, no-store, max-age=0")

	http.ServeContent(w, r, filepath.Base(f.Path), f.CreatedAt, osFile)
	return
}

func (c *Controller) Create(auto bool, message string) (bkp db_tools.Backuper, err error) {
	q := make(url.Values)
	q.Set("auto", strconv.FormatBool(auto))
	bkp = new(db_tools.Backup)
	err = c.getDb().Raw("SELECT * FROM db_backup__create(?, host(inet_client_addr()) || ?, ?)", message, ":"+c.port, q.Encode()).Scan(bkp).Error
	if err == nil {
		bkp, err = c.Get(bkp.GetID())
	}
	return
}

func (c *Controller) Remove(id db_tools.BackupID) (err error) {
	var f *File
	if f, err = c.FileOf(id); err != nil {
		return
	}
	return os.Remove(f.Path)
}

func (c *Controller) RemoveOlder(auto bool, p *db_tools.Persistence) (removed []db_tools.Backuper, err error) {
	var all []db_tools.Backuper
	err = c.List(auto, func(bkp db_tools.Backuper) error {
		all = append(all, bkp)
		return nil
	}, nil)

	if err != nil {
		return
	}

	var (
		m  = db_tools.Mapper(all)
		rm []time.Time
		g  *db_tools.IntervalGrouper
	)

	g, rm = p.Group(m.Keys())

	g.MustLast(func(t []time.Time) {
		rm = append(rm, t...)
	})

	if len(rm) > 0 {
		sort.Slice(rm, func(i, j int) bool {
			return rm[i].Before(rm[j])
		})

		mm := m.Map()

		for _, t := range rm {
			b := mm[t]
			if err = c.Remove(b.GetID()); err != nil {
				return
			}
			removed = append(removed, b)
		}
	}

	return
}

func (b *Controller) FileOf(id db_tools.BackupID) (f *File, err error) {
	err = b.listD(id.Auto, id.DbName, func(ff *File) (err error) {
		if ff.CreatedAt.Sub(id.CreatedAt) == 0 {
			f = ff
		}
		return
	}, nil)
	if err == nil && f == nil {
		err = os.ErrNotExist
	}
	return
}

func (b *Controller) Get(id db_tools.BackupID) (bkp db_tools.Backuper, err error) {
	var f *File
	if f, err = b.FileOf(id); err != nil {
		return
	}
	return f.Read()
}

func (c *Controller) List(auto bool, cb func(bkp db_tools.Backuper) error, filter db_tools.ListFilter) error {
	return c.listF(auto, filter, func(f *File) (err error) {
		var b *Backup
		if b, err = f.Read(); err != nil {
			return err
		}
		return cb(b)
	})
}

func (c *Controller) listF(auto bool, filter db_tools.ListFilter, cb func(f *File) error) (err error) {
	return c.list(auto, cb, nil)
}

func (c *Controller) listSlice(auto bool, filter func(f *File) bool) (files []*File, err error) {
	err = c.list(auto, func(f *File) error {
		files = append(files, f)
		return nil
	}, filter)
	return
}

func (c *Controller) listD(auto bool, dbName string, cb func(f *File) error, filter func(f *File) bool) (err error) {
	if filter == nil {
		filter = func(f *File) bool {
			return true
		}
	}

	root := c.GetAutoLocalDir(auto)

	err = filepath.Walk(filepath.Join(root, dbName), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				err = errListStop
			}
			return err
		}

		if !info.IsDir() {
			name := info.Name()
			if strings.HasSuffix(name, ".tar") {
				name = strings.TrimSuffix(name, ".tar")
				var t time.Time
				if t, err = time.Parse(FileNameTimeFormat, name); err == nil {
					f := &File{
						DbName:    dbName,
						Path:      path,
						Info:      info,
						CreatedAt: t,
						Auto:      auto,
					}
					if filter(f) {
						if err = cb(f); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		if err == errListStop {
			err = nil
		} else {
			return
		}
	}
	return
}

func (c *Controller) list(auto bool, cb func(f *File) error, filter func(f *File) bool) (err error) {
	if filter == nil {
		filter = func(f *File) bool {
			return true
		}
	}

	var (
		root    = c.GetAutoLocalDir(auto)
		rootF   *os.File
		dbNames []os.FileInfo
	)

	if rootF, err = os.Open(root); err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}

	if dbNames, err = rootF.Readdir(0); err != nil {
		return
	}

	for _, dbFI := range dbNames {
		if !dbFI.IsDir() {
			continue
		}
		dbName := dbFI.Name()
		if dbName[0] == '.' {
			continue
		}

		if err = c.listD(auto, dbName, cb, filter); err != nil {
			return
		}
	}
	return
}

func (c *Controller) Count(auto bool, filter db_tools.ListFilter) (count int, err error) {
	err = c.listF(auto, filter, func(*File) error {
		count++
		return nil
	})
	return
}

func New(db *gorm.DB, i18nb *i18n.Builder) *Controller {
	ConfigureMessages(i18nb)
	return &Controller{
		db:       db,
		dir:      "/backup",
		localDir: "data/db-backup",
		log:      web.NewLogger("db-pg-utils:storage_server"),
	}
}

func (b *Controller) Install() (err error) {
	if err = b.db.Exec(createSQL).Error; err != nil {
		return err
	}
	b.startSrv()
	return nil
}

func (b *Controller) startSrv() {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	// Get the actual port the server is listening on
	addr := listener.Addr().(*net.TCPAddr)
	port := addr.Port

	go func() {
		defer listener.Close()
		b.port = strconv.Itoa(port)
		b.log.Info(fmt.Sprintf("Listening on port: %d", port))

		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			fmt.Println("@@@@@@@@@@@@@@")
			q := r.URL.Query()
			tsi, _ := strconv.ParseInt(q.Get("ts"), 10, 64)
			ts := time.Unix(tsi, 0).UTC()
			pth := filepath.Join(b.GetAutoLocalDir(q.Get("auto") == "true"), q.Get("db"), ts.Format("2006/01/02"), ts.Format("20060102T150405")+".tar")
			err := os.MkdirAll(filepath.Dir(pth), 0770)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			f, err := os.OpenFile(pth, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			defer func() {
				f.Close()
				if err != nil {
					os.Remove(pth)
				}
			}()

			tw := tar.NewWriter(f)

			message := []byte(q.Get("message"))
			if err = tw.WriteHeader(&tar.Header{
				Name:    "message.txt",
				Mode:    0600,
				Size:    int64(len(message)),
				ModTime: ts,
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if _, err = tw.Write(message); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			esize, _ := strconv.ParseInt(q.Get("size"), 10, 64)

			if err = tw.WriteHeader(&tar.Header{
				Name:    "backup.pg",
				Mode:    0600,
				Size:    esize,
				ModTime: ts,
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			n, err := io.Copy(tw, r.Body)
			fmt.Println(esize, n)
			size := fmt.Sprint(n)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			} else if n != esize {
				http.Error(w, fmt.Sprintf("bad received size. expected %s, but got %s", esize, size), http.StatusInternalServerError)
			}
		})
		http.Serve(listener, mux)
	}()
}
