package context

import (
	_ "github.com/vesoft-inc/nebula-go/v2/nebula"
)

type BackendUploadTracker interface {
	StorageUploadingReport(spaceid string, host string, paths []string, desturl string)
	MetaUploadingReport(host string, paths []string, desturl string)
}

// NB - not thread-safe
type Context struct {
	LocalAddr  string // the address of br client
	RemoteAddr string // the address of nebula service
	Reporter   BackendUploadTracker
}

func NewContext(localaddr string, r BackendUploadTracker) *Context {
	return &Context{LocalAddr: localaddr, Reporter: r}
}
