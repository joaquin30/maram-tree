/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package crdt

import (
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
	"log"
	"time"
)

const (
	opAdd      = 0
	opRemove   = 1
	opUpMove   = 2
	opDownMove = 3
)

type Operation struct {
	// Para omitir campos en blanco
	_msgpack struct{} `msgpack:",omitempty"`

	OpType        uint8
	ReplicaID     uint64
	Timestamp     [NumReplicas]uint64
	Node          uuid.UUID
	Parent        uuid.UUID
	NewParent     uuid.UUID
	CritAncestors map[uuid.UUID]bool
	Descendants   map[uuid.UUID]bool
	Name          string
	Time          time.Time
	ignored       bool
}

func CritAncestorsOverlap(op1, op2 *Operation) bool {
	return op1.CritAncestors[op2.Node] && op2.CritAncestors[op1.Node]
}

// Se usa MessagePack para serializar las operaciones
func OperationFromBytes(data []byte) Operation {
	var op Operation
	err := msgpack.Unmarshal(data, &op)
	if err != nil {
		log.Println("Error decoding MessagePack")
		log.Println(string(data))
		log.Fatal(err)
	}

	return op
}

func OperationToBytes(op Operation) []byte {
	data, err := msgpack.Marshal(op)
	if err != nil {
		log.Fatal(err)
	}

	return data
}

func TimeBefore(t1, t2 [NumReplicas]uint64) bool {
	for i := range t1 {
		if t1[i] < t2[i] {
			return true
		} else if t2[i] < t1[i] {
			return false
		}
	}

	return true
}

func TimeCausal(t1, t2 [NumReplicas]uint64) bool {
	flag := false
	for i := range t1 {
		if t1[i] > t2[i] {
			return false
		}

		flag = flag || t1[i] < t2[i]
	}

	return flag
}

func TimeConcurrent(t1, t2 [NumReplicas]uint64) bool {
	return !TimeCausal(t1, t2) && !TimeCausal(t2, t1)
}

func Max(x, y uint64) uint64 {
	if x >= y {
		return x
	}

	return y
}

func Min(x, y uint64) uint64 {
	if x <= y {
		return x
	}

	return y
}
