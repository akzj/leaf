package mqtt_broker

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type Sub struct {
	id    int64
	topic string
}

func newSub(id int64, topic string) *Sub {
	return &Sub{
		id:    id,
		topic: topic,
	}
}

func (s *Sub) ID() int64 {
	return s.id
}

func (s *Sub) Topic() string {
	return s.topic
}

func subMapIDs(subMaps []map[int64]Subscriber) []int64 {
	var ids []int64
	for _, subMap := range subMaps {
		for _, s := range subMap {
			ids = append(ids, s.ID())
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids
}

func TestNextToken(t *testing.T) {
	for token, remain := nextToken("/1/2/3/4/5/6/7"); len(token) != 0 || len(remain) != 0; token, remain = nextToken(remain) {
		fmt.Println(token)
	}
}

func TestNewTree(t *testing.T) {
	tree := NewTree()

	s := newSub(1, "1/2/3/4/5")
	tree.Insert(s)

	s2 := newSub(2, "1/+/3/4/5")
	tree.Insert(s2)

	s3 := newSub(3, "1/2/+/4/5")
	tree.Insert(s3)

	s4 := newSub(4, "1/2/#")
	tree.Insert(s4)

	s5 := newSub(5, "1/2/+/+/+")
	tree.Insert(s5)

	s6 := newSub(6, "1/2/+/+/5")
	tree.Insert(s6)

	s7 := newSub(7, "1/2/+/+/5/+")
	tree.Insert(s7)

	s8 := newSub(8, "#")
	tree.Insert(s8)

	if ids := subMapIDs(tree.Match("1/2/3/4/5")); reflect.DeepEqual(ids, []int64{1, 2, 3, 4, 5, 6, 7, 8}) == false {
		t.Fatal(ids)
	}
}

func TestMatch(t *testing.T) {
	tree := NewTree()
	tree.Insert(newSub(1, "+/+"))
	tree.Insert(newSub(2, "/+"))
	tree.Insert(newSub(3, "/+/#"))
	tree.Insert(newSub(4, "/+/+"))

	if ids := subMapIDs(tree.Match("/hello")); reflect.DeepEqual(ids, []int64{1, 2, 3, 4}) == false {
		t.Fatal(ids)
	}
}

func TestClone(t *testing.T) {
	tree := NewTree()
	tree.Insert(newSub(1, "1/2/3/4/5/6"))
	tree.Insert(newSub(2, "1/2/3/4/5"))
	tree.Insert(newSub(3, "1/2/3/4"))
	tree.Insert(newSub(4, "1/2/3"))
	tree.Insert(newSub(5, "1/2"))
	tree.Insert(newSub(6, "1"))
	tree.Insert(newSub(7, "/1"))
	tree.Insert(newSub(8, "//1"))
	tree.Insert(newSub(9, "///1"))

	tree.Walk(func(path string, subscriber map[int64]Subscriber) bool {
		fmt.Println(subMapIDs([]map[int64]Subscriber{subscriber}), path)
		return true
	})
	clone := tree.Clone()
	clone.Insert(newSub(10, "////1"))
	fmt.Println(strings.Repeat("-", 100))
	tree.Walk(func(path string, subscriber map[int64]Subscriber) bool {
		fmt.Println(subMapIDs([]map[int64]Subscriber{subscriber}), path)
		return true
	})

	fmt.Println(strings.Repeat("-", 100))
	clone.Walk(func(path string, subscriber map[int64]Subscriber) bool {
		fmt.Println(subMapIDs([]map[int64]Subscriber{subscriber}), path)
		return true
	})
}

func TestDelete(t *testing.T) {
	tree := NewTree()

	s1 := newSub(1, "1/2/3/4/5/6")
	s2 := newSub(2, "1/2/3/4/5")
	s3 := newSub(3, "1/2/3/4")
	tree.Insert(s1)
	tree.Insert(s2)
	tree.Insert(s3)
	clone := tree.Clone()

	tree.Delete(s1)
	if len(tree.root.next) == 0 {
		t.Fatal("tree.root.next empty")
	}
	tree.Delete(s2)
	if len(tree.root.next) == 0 {
		t.Fatal("tree.root.next empty")
	}
	tree.Delete(s3)
	if len(tree.root.next) != 0 {
		t.Fatal("tree.root.next no empty")
	}
	if len(clone.root.next) == 0 {
		t.Fatal("clone.root.next empty")
	}
}
