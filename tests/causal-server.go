/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
 
package main

import (
	"os"
	"log"
	"net"
	"github.com/vmihailenco/msgpack/v5"
)

type message struct {
	id   int
	data []byte
}

var (
	connected [10]bool
	conn      [10]net.Conn
	queue     chan message
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("USE: ./causal-server [PORT]")
	}
	
	ln, err := net.Listen("tcp", ":"+os.Args[1])
	if err != nil {
		panic(err)
	}

	queue = make(chan message, 100000)
	go processQueue()

	for {
		c, err := ln.Accept()
		if err != nil {
			panic(err)
		}

		for id, v := range connected {
			if !v {
				connected[id] = true
				conn[id] = c
				log.Println("Connected replica", id)
				go handleConnection(id)
				break
			}
		}
	}
}

func handleConnection(id int) {
	dec := msgpack.NewDecoder(conn[id])
	for {
		data, err := dec.DecodeRaw()
		if err != nil {
			break
		}

		queue <- message{id, data}
	}

	connected[id] = false
	conn[id].Close()
	log.Println("Disconnected replica", id)
}

func processQueue() {
	for {
		select {
		case msg := <-queue:
			// log.Println(string(msg.data))
			for id, v := range connected {
				if v && id != msg.id {
					_, err := conn[id].Write(msg.data)
					if err != nil {
						connected[id] = false
					}
				}
			}
		}
	}
}
