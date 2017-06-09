package main

type intMessage struct {
	I int `json:"i"`
}

type deliveredMessage struct {
	ID    uint64 `json:"id"`
	Undel uint64 `json:"undel"`
}

//MessageSet is a set of messages
type MessageSet struct {
	Set            map[uint64]struct{} `json:"set"`
	EarliestUnseen uint64              `json:"earliest"`
}

//NewMessageSet returns a new empty messageSet
func NewMessageSet() MessageSet {
	return MessageSet{Set: make(map[uint64]struct{}), EarliestUnseen: 1}
}

//Add adds a new message to the set and compacts the set if possible
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
	for i := s.EarliestUnseen; i < msgID; i++ {
		delete(s.Set, i)
	}
	s.EarliestUnseen = msgID
}

//Contains returns true iff the set contains the message id
func (s *MessageSet) Contains(id uint64) bool {
	_, found := s.Set[id]
	return s.EarliestUnseen > id || found
}

func (s *MessageSet) size() int {
	return len(s.Set)
}
