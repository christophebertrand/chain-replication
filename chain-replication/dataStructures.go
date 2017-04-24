package main

type MessageSet struct {
	set map[uint64]struct{}
}

func NewMessageSet() *MessageSet {
	return &MessageSet{make(map[uint64]struct{})}
}

func (set *MessageSet) Add(msgID uint64) bool {
	_, found := set.set[msgID]
	set.set[msgID] = struct{}{}
	return !found //False if it existed already
}

func (set *MessageSet) Contains(id uint64) bool {
	_, found := set.set[id]
	return found //true if it existed already
}

func (set *MessageSet) Remove(id uint64) {
	delete(set.set, id)
}

func (set *MessageSet) Size() int {
	return len(set.set)
}

type PeerSet struct {
	set map[peer]struct{}
}

func NewPeerSet() *PeerSet {
	return &PeerSet{make(map[peer]struct{})}
}

func (set *PeerSet) Add(i peer) bool {
	_, found := set.set[i]
	var a struct{}
	set.set[i] = a
	return !found //False if it existed already
}

func (set *PeerSet) Contains(i peer) bool {
	_, found := set.set[i]
	return found //true if it existed already
}

func (set *PeerSet) Remove(i peer) {
	delete(set.set, i)
}

func (set *PeerSet) Size() int {
	return len(set.set)
}
