package simple_conn_pool

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

//TODO: 保存所有的连接, 而不是只保存连接计数

var ErrMaxConn = fmt.Errorf("maximum connections reached")

//
type NConn interface {
	io.Closer
	Name() string
	Closed() bool
}

type ConnPool struct {
	sync.RWMutex

	Name     string
	Address  string
	MaxConns int32
	MaxIdle  int32
	Cnt      int64
	New      func(name string) (NConn, error)

	active   int32
	free     []NConn
	freeLock *sync.RWMutex
	all      map[string]NConn
}

func NewConnPool(name string, address string, maxConns int32, maxIdle int32) *ConnPool {
	return &ConnPool{Name: name, Address: address, MaxConns: maxConns, MaxIdle: maxIdle, Cnt: 0, all: make(map[string]NConn), freeLock: &sync.RWMutex{}}
}

func (this *ConnPool) Proc() string {
	this.RLock()
	defer this.RUnlock()

	return fmt.Sprintf("Name:%s,Cnt:%d,active:%d,all:%d,free:%d",
		this.Name, this.Cnt, this.active, len(this.all), len(this.free))
}

func (this *ConnPool) Fetch() (NConn, error) {

	// get from free
	conn := this.fetchFree()
	if conn != nil {
		return conn, nil
	}

	if this.overMax() {
		return nil, ErrMaxConn
	}

	// create new conn
	conn, err := this.newConn()
	if err != nil {
		return nil, err
	}

	this.increActive()
	return conn, nil
}

func (this *ConnPool) Release(conn NConn) {

	if this.overMaxIdle() {
		this.deleteConn(conn)
		this.decreActive()
	} else {
		this.addFree(conn)
	}
}

func (this *ConnPool) ForceClose(conn NConn) {
	this.deleteConn(conn)
	this.decreActive()
}

func (this *ConnPool) Destroy() {

	this.Lock()
	defer this.Unlock()

	for _, conn := range this.free {
		if conn != nil && !conn.Closed() {
			conn.Close()
		}
	}

	for _, conn := range this.all {
		if conn != nil && !conn.Closed() {
			conn.Close()
		}
	}

	this.active = 0
	this.free = []NConn{}
	this.all = map[string]NConn{}
}

// internal, concurrently unsafe
func (this *ConnPool) newConn() (NConn, error) {
	name := fmt.Sprintf("%s_%d_%d", this.Name, this.Cnt, time.Now().UnixNano())
	conn, err := this.New(name)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, err
	}

	atomic.AddInt64(&this.Cnt, 1)

	this.Lock()
	this.all[conn.Name()] = conn
	this.Unlock()

	return conn, nil
}

func (this *ConnPool) deleteConn(conn NConn) {
	this.Lock()
	if conn != nil {
		conn.Close()
	}
	delete(this.all, conn.Name())
	this.Unlock()
	atomic.AddInt64(&this.Cnt, -1)
}

func (this *ConnPool) addFree(conn NConn) {
	this.freeLock.Lock()
	this.free = append(this.free, conn)
	this.freeLock.Unlock()
}

func (this *ConnPool) fetchFree() NConn {

	this.freeLock.Lock()
	defer this.freeLock.Unlock()
	if len(this.free) == 0 {
		return nil
	}
	conn := this.free[0]
	this.free = this.free[1:]
	return conn
}

func (this *ConnPool) increActive() {
	atomic.AddInt32(&this.active, 1)
}

func (this *ConnPool) decreActive() {
	atomic.AddInt32(&this.active, -1)
}

func (this *ConnPool) overMax() bool {
	return this.active >= this.MaxConns
}

func (this *ConnPool) overMaxIdle() bool {
	return int32(len(this.free)) >= this.MaxIdle
}
