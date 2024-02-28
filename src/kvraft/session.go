package kvraft

type sessionResult struct {
	LastCommandId int
	Value         string
	Err           Err
}

type Session struct {
	Store map[string]sessionResult
}

func NewSession() Session {
	return Session{
		Store: make(map[string]sessionResult),
	}
}

func (s *Session) getSessionResult(clientUUID string) sessionResult {
	v, ok := s.Store[clientUUID]
	if !ok {
		return sessionResult{}
	}
	return v
}

func (s *Session) setSessionResult(clientUUID string, res sessionResult) {
	s.Store[clientUUID] = res
}
