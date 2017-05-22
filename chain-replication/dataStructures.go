package main

type MessageSet struct {
	Set            map[uint64]struct{} `json:"set"`
	EarliestUnseen uint64              `json:"earliest"`
	// mu             sync.RWMutex
}

func NewMessageSet() MessageSet {
	return MessageSet{Set: make(map[uint64]struct{}), EarliestUnseen: 1}
}

func (s *MessageSet) Add(msgID uint64) {
	if msgID < s.EarliestUnseen {
		return
	}
	if msgID == s.EarliestUnseen {
		s.EarliestUnseen++
		maxRange := s.EarliestUnseen + uint64(s.size())
		for i := s.EarliestUnseen; i < maxRange; i++ {
			if s.Contains(i) {
				delete(s.Set, i)
				s.EarliestUnseen++
			} else {
				return
			}
		}
	} else {
		s.Set[msgID] = struct{}{}
	}
}

//AddUntil adds id from 0 to msgID (msgID excluded) to the set
func (s *MessageSet) AddUntil(msgID uint64) {
	if s.EarliestUnseen > msgID {
		return
	}
	// s.mu.Lock()
	// defer s.mu.Unlock()
	for i := s.EarliestUnseen; i < msgID; i++ {
		delete(s.Set, i)
	}
	s.EarliestUnseen = msgID
}

func (s *MessageSet) Contains(id uint64) bool {
	// s.mu.RLock()
	// defer s.mu.RUnlock()
	_, found := s.Set[id]
	return s.EarliestUnseen > id || found
}

//func (s *MessageSet) Remove(id uint64) {
//	delete(s.set, id)
//}

func (s *MessageSet) size() int {
	// s.mu.RLock()
	// defer s.mu.RUnlock()
	return len(s.Set)
}
