package topic

import (
	"sort"
	"strings"
)

type Subscriber interface {
	ID() int64
	Topic() string
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
	return clone
}

type Node struct {
	CoW         *copyOnWrite
	token       string
	path        string
	next        map[string]*Node
	subscribers map[int64]Subscriber
}

func newNode(token string) *Node {
	return &Node{
		token:       token,
		next:        make(map[string]*Node),
		subscribers: make(map[int64]Subscriber),
	}
}

func (n *Node) insert(level int, token string, remain string, path string, sub Subscriber) {
	if token == "" && remain == "" {
		n.subscribers[sub.ID()] = sub
		return
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
	next.insert(level+1, token, remain, path, sub)
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

type Tree struct {
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

func NewTree() *Tree {
	Cow := new(copyOnWrite)
	root := newNode("")
	root.CoW = Cow
	return &Tree{
		cow:  Cow,
		root: root,
	}
}

func (tree *Tree) Insert(sub Subscriber) {
	token, remain := nextToken(sub.Topic())
	node := tree.cow.mutableNode(tree.root)
	node.insert(0, token, remain, "", sub)
	tree.root = node
}

func (tree *Tree) Match(topic string) []map[int64]Subscriber {
	token, remain := nextToken(topic)
	return tree.root.match(token, remain)
}

func (tree *Tree) Clone() *Tree {
	cow := *tree.cow
	tree.cow = &cow
	clone := &Tree{cow: new(copyOnWrite), root: tree.root}
	return clone
}

func (tree *Tree) Walk(f func(path string, subscriber map[int64]Subscriber) bool) {
	tree.root.walk(f)
}
