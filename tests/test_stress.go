/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package main

import (
	"errors"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
	"maram-tree/crdt"
	"github.com/google/uuid"
)

const (
	total = 60
)

func main() {
	if len(os.Args) != 4 {
		log.Fatal(errors.New("USE: ./test [id] [server_ip] [ops]"))
	}

	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}

	ops, err := strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}

	log.SetPrefix("CRDT " + os.Args[1] + " ")
	tree := crdt.NewTree(id, os.Args[2])
	go func() {
		time.Sleep(total * time.Second)
		log.Println("\n\tSeconds running:", total,
			"\n\tOperations per second:", (tree.LocalCnt+tree.RemoteCnt)/total,
			"\n\tAvg delay for local operation:", tree.LocalSum/time.Duration(tree.LocalCnt),
			"\n\tAvg delay for remote operation:", tree.RemoteSum/time.Duration(tree.RemoteCnt),
			"\n\tAvg undo and redos for remote operation:", tree.UndoRedoCnt/tree.RemoteCnt,
			"\n\tAvg bandwidth consumption:", tree.PacketSzSum/total, "bps")
		os.Exit(0)
	}()

	GenerateLoad(tree, ops)
}

func GenerateLoad(tree *crdt.Tree, ops int) {
	nodes := []string{"root"}
	rate := time.Second / time.Duration(ops)
	previous := time.Now()
	var delay time.Duration
	for {
		current := time.Now()
		delay += current.Sub(previous)
		previous = current

		for delay >= rate {
			// Solo se añaden o se mueven nodos con probabilidad 50%
			// No se eliminan
			delay -= rate
			x := rand.Intn(2)
			if x == 0 {
				i := rand.Intn(len(nodes))
				name := uuid.New().String()
				nodes = append(nodes, name)
				tree.Add(name, nodes[i])
			} else {
				i := rand.Intn(len(nodes))
				j := rand.Intn(len(nodes))
				tree.Move(nodes[i], nodes[j])
			}
		}
	}
}
