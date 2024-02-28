package kvraft

// 保证串行化 不需要上锁
type StateMechine struct {
	Store map[string]string
}

func NewStateMechine() StateMechine {
	return StateMechine{
		Store: make(map[string]string),
	}
}

func (sm *StateMechine) Get(key string) string {
	return sm.Store[key]
}
func (sm *StateMechine) Put(key, value string) {
	sm.Store[key] = value
}
func (sm *StateMechine) Append(key, value string) {
	sm.Store[key] += value
}

// func (sm *StateMechine) ApplyOp(op Op) OperateReply {
// 	switch op.Opt {
// 	case OptGet:
// 		return OperateReply{
// 			Err:   OK,
// 			Value: sm.Get(op.Key),
// 		}
// 	case OptPut:
// 		sm.Put(op.Key, op.Value)
// 	case OptAppend:
// 		sm.Append(op.Key, op.Value)
// 	}

// 	return OperateReply{
// 		Err:   OK,
// 		Value: "",
// 	}
// }
