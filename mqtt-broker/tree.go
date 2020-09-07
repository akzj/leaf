// Copyright 2020-2026 The streamIO Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt_broker

import (
	"github.com/eclipse/paho.mqtt.golang/packets"
	"sort"
	"strings"
)

type Subscriber interface {
	ID() int64
	Topic() string
	Qos() int32
	Online() bool
	writePacket(packet *packets.PublishPacket, callback func(err error))
}

type copyOnWrite struct {
	_ int
}

func (CoW *copyOnWrite) mutableNode(node *Node) *Node {
	if CoW == node.CoW {
		return node
	}
	clone := newNode(node.token)
	for ID, sub := range node.subscribers {
		clone.subscribers[ID] = sub
	}
	for topic, next := range node.next {
		clone.next[topic] = next
	}
	clone.CoW = CoW
	clone.path = node.path
	clone.retain = node.retain
	return clone
}

type Node struct {
	CoW         *copyOnWrite
	token       string
	path        string
	next        map[string]*Node
	subscribers map[int64]Subscriber
	retain      *packets.PublishPacket
}

func newNode(token string) *Node {
	return &Node{
		token:       token,
		next:        make(map[string]*Node),
		subscribers: make(map[int64]Subscriber),
	}
}

func (n *Node) getOrCreateNode(level int, token string, remain string, path string) *Node {
	if token == "" && remain == "" {
		return n
	}
	if level > 0 {
		path = path + "/" + token
	} else {
		path = token
	}
	next, ok := n.next[token]
	if ok == false {
		next = newNode(token)
		next.CoW = n.CoW
		next.path = path
		n.next[token] = next
	} else {
		if next.CoW != n.CoW {
			next = n.CoW.mutableNode(next)
			n.next[token] = next
		}
	}
	token, remain = nextToken(remain)
	return next.getOrCreateNode(level+1, token, remain, path)
}

func (n *Node) match(token string, remain string) []map[int64]Subscriber {
	var subMaps []map[int64]Subscriber
	if len(token) == 0 && len(remain) == 0 {
		if next, ok := n.next["+"]; ok && len(next.subscribers) != 0 {
			subMaps = append(subMaps, next.subscribers)
		}
		if next, ok := n.next["#"]; ok && len(next.subscribers) != 0 {
			subMaps = append(subMaps, next.subscribers)
		}
		if len(n.subscribers) != 0 {
			subMaps = append(subMaps, n.subscribers)
		}
		return subMaps
	}
	if next, ok := n.next[token]; ok {
		token, remain := nextToken(remain)
		if sMaps := next.match(token, remain); sMaps != nil {
			subMaps = append(subMaps, sMaps...)
		}
	}
	if next, ok := n.next["+"]; ok {
		token, remain := nextToken(remain)
		if sMaps := next.match(token, remain); sMaps != nil {
			subMaps = append(subMaps, sMaps...)
		}
	}

	if next, ok := n.next["#"]; ok && len(next.subscribers) != 0 {
		subMaps = append(subMaps, next.subscribers)
	}
	return subMaps
}

func (n *Node) rangeRetainMessage(f func(packet *packets.PublishPacket) bool) bool {
	if n.retain != nil {
		if f(n.retain) == false {
			return false
		}
	}
	for _, next := range n.next {
		if next.rangeRetainMessage(f) == false {
			return false
		}
	}
	return true
}

func (n *Node) walk(f func(path string, subscribers map[int64]Subscriber) bool) bool {
	if len(n.subscribers) != 0 {
		if f(n.path, n.subscribers) == false {
			return false
		}
	}
	var nodes []*Node
	for _, next := range n.next {
		nodes = append(nodes, next)
	}
	sort.Slice(nodes, func(i, j int) bool {
		if len(nodes[i].path) != len(nodes[j].path) {
			return len(nodes[i].path) < len(nodes[j].path)
		}
		return nodes[i].path < nodes[j].path
	})
	for _, next := range nodes {
		if next.walk(f) == false {
			return false
		}
	}
	return true
}

func (n *Node) mutableNext(token string) *Node {
	next, ok := n.next[token]
	if ok == false {
		return nil
	}
	if next.CoW != n.CoW {
		next = n.CoW.mutableNode(next)
		n.next[token] = next
	}
	return next
}

type TopicTree struct {
	cow  *copyOnWrite
	root *Node
}

func nextToken(topic string) (token string, remain string) {
	index := strings.IndexByte(topic, '/')
	if index == -1 {
		return topic, ""
	}
	return topic[:index], topic[index+1:]
}

func NewTopicTree() *TopicTree {
	Cow := new(copyOnWrite)
	root := newNode("")
	root.CoW = Cow
	return &TopicTree{
		cow:  Cow,
		root: root,
	}
}

func (tree *TopicTree) Insert(sub Subscriber) {
	token, remain := nextToken(sub.Topic())
	tree.root = tree.cow.mutableNode(tree.root)
	tree.root.getOrCreateNode(0, token, remain, "").
		subscribers[sub.ID()] = sub
}

func (tree *TopicTree) UpdateRetainPacket(packet *packets.PublishPacket) {
	token, remain := nextToken(packet.TopicName)
	tree.root = tree.cow.mutableNode(tree.root)
	node := tree.root.getOrCreateNode(0, token, remain, "")
	if len(packet.Payload) == 0 {
		node.retain = nil
	} else {
		node.retain = packet
	}
}

func (tree *TopicTree) Match(topic string) []map[int64]Subscriber {
	token, remain := nextToken(topic)
	return tree.root.match(token, remain)
}

func (tree *TopicTree) Clone() *TopicTree {
	cow := *tree.cow
	tree.cow = &cow
	clone := &TopicTree{cow: new(copyOnWrite), root: tree.root}
	return clone
}

func (tree *TopicTree) Walk(f func(path string, subscribers map[int64]Subscriber) bool) {
	tree.root.walk(f)
}
func (tree *TopicTree) RangeRetainMessage(f func(packet *packets.PublishPacket) bool) {
	tree.root.rangeRetainMessage(f)
}

type nodeStack []*Node

func (stack *nodeStack) Push(node *Node) {
	*stack = append(*stack, node)
}

func (stack *nodeStack) Pop() *Node {
	if len(*stack) == 0 {
		return nil
	}
	node := (*stack)[len(*stack)-1]
	*stack = (*stack)[:len(*stack)-1]
	return node
}

func (tree *TopicTree) Delete(subscriber Subscriber) {
	tree.root = tree.cow.mutableNode(tree.root)
	next := tree.root
	var stack = new(nodeStack)
	stack.Push(next)
	for token, remain := nextToken(subscriber.Topic()); len(token) != 0 || len(remain) != 0; token, remain = nextToken(remain) {
		if next = next.mutableNext(token); next == nil {
			return
		}
		stack.Push(next)
	}
	delete(next.subscribers, subscriber.ID())
	preNode := next
	stack.Pop()
	for node := stack.Pop(); node != nil; node = stack.Pop() {
		if len(preNode.subscribers) > 0 {
			return
		}
		delete(node.next, preNode.token)
		preNode = node
	}
}
