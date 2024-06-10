/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package crdt

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log"
	"maram-tree/network"
	"sort"
	"sync"
	"time"
)

const (
	rootName    = "root"
	NumReplicas = 3
)

var (
	rootID = uuid.MustParse("5568143b-b80b-452d-ba1b-f9d333a06e7a")
)

type treeNode struct {
	id       uuid.UUID
	name     string
	parent   *treeNode
	deleted  bool
	children []*treeNode
}

type Tree struct {
	sync.Mutex
	id        uint64
	time      [NumReplicas]uint64 // vector clock
	logTime   [NumReplicas][NumReplicas]uint64
	nodes     map[uuid.UUID]*treeNode
	names     map[string]uuid.UUID
	root      *treeNode
	conn      network.ReplicaConn
	history   []Operation
	ignored   []Operation
	// Estadisticas
	LocalSum    time.Duration
	LocalCnt    uint64
	RemoteSum   time.Duration
	RemoteCnt   uint64
	UndoRedoCnt uint64
	PacketSzSum uint64
}

func NewTree(id int, serverIP string) *Tree {
	tree := Tree{}
	tree.id = uint64(id)
	tree.nodes = make(map[uuid.UUID]*treeNode)
	tree.names = make(map[string]uuid.UUID)

	if NumReplicas < id {
		panic("Tree CRDT: Invalid ID")
	}

	tree.names[rootName] = rootID
	tree.nodes[rootID] = &treeNode{id: rootID, name: rootName}
	tree.root = tree.nodes[rootID]

	tree.conn = network.NewCausalConn(&tree, serverIP)
	// Esperar a que las demas replicas se inicien
	time.Sleep(5 * time.Second)

	// Iniciando corutina que cada segundo limpiará el historial
	go func() {
		for range time.Tick(time.Second) {
			tree.truncateHistory()
		}
	}()

	return &tree
}

func (this *Tree) GetID() int {
	return int(this.id)
}

func (this *Tree) ApplyRemoteOperation(data []byte) {
	this.Lock()
	defer this.Unlock()

	this.PacketSzSum += uint64(len(data))
	op := OperationFromBytes(data)
	op.Time = time.Now()
	this.apply(op)
	for i := range this.time {
		this.time[i] = Max(this.time[i], op.Timestamp[i])
	}
}

func (this *Tree) truncateHistory() {
	this.Lock()
	defer this.Unlock()

	log.Printf("History before %d\n", len(this.history))
	start := 0
	for i := range this.history {
		// fmt.Println(v)
		flag := true
		for j := range this.logTime {
			if !TimeCausal(this.history[i].Timestamp, this.logTime[j]) {
				flag = false
				break
			}
		}
		
		if flag {
			start++
		} else {
			break
		}
	}
	
	this.history = this.history[start:]
	log.Printf("History truncated to %d\n", len(this.history))
}

// checkear si node1 es descendiente de node2
func (this *Tree) isDescendant(node1, node2 *treeNode) bool {
	curr := node1
	for curr != nil {
		if curr == node2 {
			return true
		} else {
			curr = curr.parent
		}
	}

	return false
}

// checkear si node ha sido eliminado
func (this *Tree) isDeleted(node *treeNode) bool {
	curr := node
	for curr != nil {
		if curr.deleted {
			return true
		} else {
			curr = curr.parent
		}
	}

	return false
}

// obtener la distancia de node a root
func (this *Tree) getRank(node *treeNode) int {
	rank := 0
	curr := node
	for curr != nil {
		rank++
		curr = curr.parent
	}

	return rank
}

// cambiar el puntero de posicion
func (this *Tree) movePtr(node, newParent *treeNode) {
	if node == nil || node.parent == nil || newParent == nil {
		log.Fatal("movePtr: nil pointers")
	}

	for i, child := range node.parent.children {
		if child == node {
			node.parent.children[i] = node.parent.children[len(node.parent.children)-1]
			node.parent.children = node.parent.children[:len(node.parent.children)-1]
		}
	}

	node.parent = newParent
	newParent.children = append(newParent.children, node)
}

// aplicar la operacion op
// aca esta la magia, debe revertir ops del historial, aplicar la nueva op
// guardar la nueva op en el historial y reaplicar las ops del historial
// ignorando las ops invalidas
func (this *Tree) apply(op Operation) {
	switch op.OpType {
	case opAdd:
		parentPtr, ok := this.nodes[op.Parent]
		if !ok {
			log.Fatal("add: Parent does not exist", op)
		}

		this.names[op.Name] = op.Node
		this.nodes[op.Node] = &treeNode{
			id:     op.Node,
			name:   op.Name,
			parent: parentPtr,
		}
		parentPtr.children = append(parentPtr.children, this.nodes[op.Node])

	case opRemove:
		nodePtr, ok := this.nodes[op.Node]
		if !ok {
			log.Fatal("rm: Node does not exist", op)
		}

		nodePtr.deleted = true

	default:
		nodePtr, ok1 := this.nodes[op.Node]
		_, ok2 := this.nodes[op.Parent]
		newParentPtr, ok3 := this.nodes[op.NewParent]
		
		if !ok1 {
			log.Fatal("mv: Node does not exist", op)
		} else if !ok2 {
			log.Fatal("mv: Old parent does not exist", op)
		} else if !ok3 {
			log.Fatal("mv: New parent does not exist", op)
		}
		
		// Creando registro en el historial
		this.history = append(this.history, op)
		// Colocando la operacion en su lugar en el historial
		i := len(this.history) - 1
		for i > 0 && TimeBefore(this.history[i].Timestamp, this.history[i-1].Timestamp) {
			this.history[i], this.history[i-1] = this.history[i-1], this.history[i]
			i--
		}

		if this.id == op.ReplicaID || TimeCausal(this.time, op.Timestamp) {
			this.movePtr(nodePtr, newParentPtr)
			// log.Println("local", op.Timestamp, this.time)
			this.LocalCnt++
			this.LocalSum += time.Now().Sub(op.Time)
		} else {
			// log.Println("remote", op.Timestamp, this.time)
			this.UndoRedoCnt += this.reconstructTree(i)
			// log.Println("history:", this.history)
			this.RemoteCnt++
			this.RemoteSum += time.Now().Sub(op.Time)
		}
	}
	
	this.logTime[op.ReplicaID] = op.Timestamp
	if this.id == op.ReplicaID {
		// Transmision de actualizacion a otras replicas
		data := OperationToBytes(op)
		this.PacketSzSum += uint64(len(data))
		this.conn.Send(data)
	}
}

// revierte una operacion
func (this *Tree) revert(i int) {
	if this.history[i].ignored {
		return
	}

	nodePtr := this.nodes[this.history[i].Node]
	parentPtr := this.nodes[this.history[i].Parent]
	this.movePtr(nodePtr, parentPtr)
}

// reconstruye el arbol con respecto a ops concurrentes
func (this *Tree) reconstructTree(idx int) uint64 {
	start := 0
	var cnt uint64
	for i := range this.history {
		if i == idx || TimeConcurrent(this.history[i].Timestamp, this.history[idx].Timestamp) {
			start = i
			break
		}
	}

	for i := len(this.history) - 1; i >= start; i-- {
		this.revert(i)
	}

	for i := start; i < len(this.history); i++ {
		cnt++
		if this.history[i].OpType == opUpMove {
			this.applyUpMove(i)
		} else {
			this.applyDownMove(i)
		}
	}
	
	return cnt
}

func (this *Tree) getConcurrent(idx int) []int {
	var concurrent []int
	for i := range this.history {
		if TimeConcurrent(this.history[i].Timestamp, this.history[idx].Timestamp) {
			concurrent = append(concurrent, i)
		}
	}

	return concurrent
}

func (this *Tree) getHistoricalIgnored(idx int) []int {
	var historical []int
	for i := range this.history {
		if i >= idx {
			break
		}
		
		if this.history[i].ignored && TimeCausal(this.history[i].Timestamp, this.history[idx].Timestamp) {
			historical = append(historical, i)
		}
	}

	return historical
}

func (this *Tree) applyUpMove(i int) bool {
	op1 := &this.history[i]
	for _, j := range this.getConcurrent(i) {
		op2 := &this.history[j]
		// up-move ∧ op.params.n = n ∧ op.priority > priority
		if op2.OpType == opUpMove && op2.Node == op1.Node && op2.ReplicaID > op1.ReplicaID {
			op1.ignored = true
			return false
		}
	}

	for _, j := range this.getHistoricalIgnored(i) {
		op2 := &this.history[j]
		if !TimeCausal(op2.Timestamp, op1.Timestamp) {
			continue
		}

		// op.type = up-move ∧ p′ ∈ self-or-under(op.params.n)
		if op2.OpType == opUpMove && op2.Descendants[op1.NewParent] {
			op1.ignored = true
			return false
			// op.type = down-move ∧ (n ∈ self-or-under(op.params.n) ∨ (op.params.n ∈ self-or-under(n) ∧ p′ ∈ self-or-under(op.params.n)))
		} else if op2.OpType == opDownMove &&
			(op2.Descendants[op1.Node] ||
				(op1.Descendants[op2.Node] && op2.Descendants[op1.NewParent])) {
			op1.ignored = true
			return false
		}
	}

	this.movePtr(this.nodes[op1.Node], this.nodes[op1.NewParent])
	op1.ignored = false
	return true
}

func (this *Tree) applyDownMove(i int) bool {
	op1 := &this.history[i]
	for _, j := range this.getConcurrent(i) {
		op2 := &this.history[j]
		// op.type = up-move ∧ (crit-anc-overlap(down-move(n, p′), op) ∨ op.params.n = n)
		if op2.OpType == opUpMove && (CritAncestorsOverlap(op1, op2) || op2.Node == op1.Node) {
			op1.ignored = true
			return false
			// op.type = down-move ∧ (crit-anc-overlap(down-move(n, p′), op) ∨ op.params.n = n) ∧ op.priority > priority
		} else if op2.OpType == opDownMove &&
			(CritAncestorsOverlap(op1, op2) || op2.Node == op1.Node) &&
			op2.ReplicaID > op1.ReplicaID {
			op1.ignored = true
			return false
		}
	}

	for _, j := range this.getHistoricalIgnored(i) {
		op2 := &this.history[j]
		if !TimeCausal(op2.Timestamp, op1.Timestamp) {
			continue
		}

		// op.type = up-move ∧ (n ∈ self-or-under(op.params.n) ∨ (op.params.n ∈ self-or-under(n) ∧ p′ ∈ self-or-under(op.params.n)))
		if op2.OpType == opUpMove &&
			(op2.Descendants[op1.Node] ||
				(op1.Descendants[op2.Node] && op2.Descendants[op1.NewParent])) {
			op1.ignored = true
			return false
			// op.type = down-move ∧ p′ ∈ self-or-under(op.params.n)
		} else if op2.OpType == opDownMove && op2.Descendants[op1.NewParent] {
			op1.ignored = true
			return false
		}
	}

	this.movePtr(this.nodes[op1.Node], this.nodes[op1.NewParent])
	op1.ignored = false
	return true
}

func (this *Tree) getDescendants(node *treeNode, desc map[uuid.UUID]bool) {
	desc[node.id] = true
	for _, child := range node.children {
		this.getDescendants(child, desc)
	}
}

func (this *Tree) getCritAncestors(node *treeNode, parent *treeNode) map[uuid.UUID]bool {
	ancs := make(map[uuid.UUID]bool)

	curr := parent
	for curr != nil {
		ancs[curr.id] = true
		curr = curr.parent
	}

	curr = node
	for curr != nil {
		delete(ancs, curr.id)
		curr = curr.parent
	}

	return ancs
}

func (this *Tree) Add(name, parent string) error {
	this.Lock()
	defer this.Unlock()

	if _, ok := this.names[name]; ok {
		return errors.New("Name already exists")
	}

	parentID, ok := this.names[parent]
	if !ok || this.isDeleted(this.nodes[parentID]) {
		return errors.New("Parent does not exist")
	}
	
	this.time[this.id]++
	op := Operation{
		OpType:    opAdd,
		ReplicaID: this.id,
		Timestamp: this.time,
		Node:      uuid.New(),
		Parent:    parentID,
		Name:      name,
		Time:      time.Now(),
	}
	this.apply(op)
	return nil
}

func (this *Tree) Move(node, newParent string) error {
	this.Lock()
	defer this.Unlock()

	nodeID, ok1 := this.names[node]
	parentID, ok2 := this.names[newParent]
	if !ok1 || this.isDeleted(this.nodes[nodeID]) {
		return errors.New("Node does not exist")
	} else if !ok2 || this.isDeleted(this.nodes[parentID]) {
		return errors.New("Parent does not exist")
	} else if nodeID == rootID {
		return errors.New("Cannot move root")
	} else if this.isDescendant(this.nodes[parentID], this.nodes[nodeID]) {
		return errors.New("Cannot move node to one of its decendants")
	} else if this.nodes[nodeID].parent.id == parentID {
		return errors.New("New parent is already the parent of node")
	}

	this.time[this.id]++
	op := Operation{
		ReplicaID:     this.id,
		Timestamp:     this.time,
		Node:          nodeID,
		Parent:        this.nodes[nodeID].parent.id,
		NewParent:     parentID,
		CritAncestors: this.getCritAncestors(this.nodes[nodeID], this.nodes[parentID]),
		Descendants:   make(map[uuid.UUID]bool),
		Time:          time.Now(),
	}
	this.getDescendants(this.nodes[nodeID], op.Descendants)
	if this.getRank(this.nodes[nodeID]) > this.getRank(this.nodes[parentID]) {
		op.OpType = opUpMove
	} else {
		op.OpType = opDownMove
	}

	this.apply(op)
	return nil
}

func (this *Tree) Remove(node string) error {
	this.Lock()
	defer this.Unlock()

	nodeID, ok := this.names[node]
	if !ok || this.isDeleted(this.nodes[nodeID]) {
		return errors.New("Node does not exist")
	} else if nodeID == rootID {
		return errors.New("Cannot remove root")
	}

	this.time[this.id]++
	op := Operation{
		OpType:    opRemove,
		ReplicaID: this.id,
		Timestamp: this.time,
		Node:      nodeID,
		Time:      time.Now(),
	}
	this.apply(op)
	return nil
}

// imprimir arbol como "cmd tree"
func (this *Tree) Print() {
	this.Lock()
	defer this.Unlock()

	/* sort.Slice(this.ops, func(i, j int) bool {
		if this.ops[i].Timestamp == this.ops[j].Timestamp {
			return this.ops[i].ReplicaID < this.ops[j].ReplicaID
		}

		return this.ops[i].Timestamp < this.ops[j].Timestamp
	})

	for _, v := range this.ops {
		fmt.Println(v)
	}*/

	fmt.Println(rootName)
	printNode(this.root, "")
}

func printNode(node *treeNode, prefix string) {
	sort.Slice(node.children, func(i, j int) bool {
		return node.children[i].name < node.children[j].name
	})

	for index, child := range node.children {
		if child.deleted {
			continue
		}
		
		if index == len(node.children)-1 {
			fmt.Println(prefix+"└──", child.name)
			printNode(child, prefix+"    ")
		} else {
			fmt.Println(prefix+"├──", child.name)
			printNode(child, prefix+"│   ")
		}
	}
}

func (this *Tree) Disconnect() {
	this.conn.Disconnect()
}

func (this *Tree) Connect() {
	this.conn.Connect()
}

func (this *Tree) Close() {
	this.conn.Close()
}
