/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package network

import (
	"net"
	"sync"
	// "log"
	"github.com/vmihailenco/msgpack/v5"
)

type CausalConn struct {
	toSend    chan []byte
	toApply   chan []byte
	exit      chan bool
	connected bool
	wg        sync.WaitGroup
	conn      net.Conn
}

func NewCausalConn(tree CRDTTree, serverIP string) *CausalConn {
	replica := CausalConn{
		toSend:    make(chan []byte, 100000),
		toApply:   make(chan []byte, 100000),
		exit:      make(chan bool),
		connected: true,
	}

	conn, err := net.Dial("tcp", serverIP)
	if err != nil {
		panic(err)
	}

	replica.conn = conn
	go replica.receiveOperations()
	go replica.processToSend()
	go replica.processToApply(tree)
	return &replica
}

func (this *CausalConn) Send(data []byte) {
	this.toSend <- data
}

func (this *CausalConn) Disconnect() {
	if this.connected {
		this.connected = false
		this.exit <- true
	}
}

func (this *CausalConn) Connect() {
	if !this.connected {
		this.connected = true
		go this.processToSend()
	}
}

func (this *CausalConn) Close() {
	close(this.toSend)
	this.wg.Wait()
}

func (this *CausalConn) processToSend() {
	this.wg.Add(1)
	defer this.wg.Done()

	for {
		select {
		case data, ok := <-this.toSend:
			if !ok {
				return
			}

			this.conn.Write(data)

		case <-this.exit:
			return
		}
	}
}

func (this *CausalConn) receiveOperations() {
	dec := msgpack.NewDecoder(this.conn)
	for {
		data, err := dec.DecodeRaw()
		if err != nil {
			panic(err)
		}

		//log.Println("RECV: " + string(data))
		this.toApply <- data
	}
}

func (this *CausalConn) processToApply(tree CRDTTree) {
	for {
		select {
		case data := <-this.toApply:
			tree.ApplyRemoteOperation(data)
		}
	}
}
