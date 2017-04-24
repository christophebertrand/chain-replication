package main

import "sync"

type MessageSet struct {
	set            map[uint64]struct{}
	earliestUnseen uint64
	mu             sync.RWMutex
}

func NewMessageSet() *MessageSet {
	return &MessageSet{set: make(map[uint64]struct{}), earliestUnseen: 0}
}

func (s *MessageSet) Add(msgID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if msgID == s.earliestUnseen {
		s.earliestUnseen++
		maxRange := s.earliestUnseen + uint64(s.size())
		for i := s.earliestUnseen; i < maxRange; i++ {
			if s.Contains(i) {
				delete(s.set, msgID)
				s.earliestUnseen++
			} else {
				return
			}
		}
	} else {
		s.set[msgID] = struct{}{}
	}
}

//adds id from 0 to msgID (msgID excluded) to the set
func (s *MessageSet) AddUntil(msgID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := s.earliestUnseen; i < msgID; i++ {
		delete(s.set, i)
	}
	s.earliestUnseen = msgID
}

func (s *MessageSet) Contains(id uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, found := s.set[id]
	return s.earliestUnseen > id || found
}

//func (s *MessageSet) Remove(id uint64) {
//	delete(s.set, id)
//}

func (s *MessageSet) size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.set)
}

//
//type PeerSet struct {
//	set map[peer]struct{}
//}
//
//func NewPeerSet() *PeerSet {
//	return &PeerSet{make(map[peer]struct{})}
//}
//
//func (set *PeerSet) Add(i peer) bool {
//	_, found := set.set[i]
//	var a struct{}
//	set.set[i] = a
//	return !found //False if it existed already
//}
//
//func (set *PeerSet) Contains(i peer) bool {
//	_, found := set.set[i]
//	return found //true if it existed already
//}
//
//func (set *PeerSet) Remove(i peer) {
//	delete(set.set, i)
//}
//
//func (set *PeerSet) Size() int {
//	return len(set.set)
//}
