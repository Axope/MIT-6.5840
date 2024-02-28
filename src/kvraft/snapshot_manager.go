package kvraft

import (
	"bytes"

	"6.5840/labgob"
)

func (kv *KVServer) InstallSnapshot(data []byte, snapshotIndex int) {
	defer kv.Logger.Sync()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var stateMechine StateMechine
	var session Session
	if d.Decode(&stateMechine) != nil || d.Decode(&session) != nil {
		kv.Logger.Error("InstallSnapshot error")
		return
	} else {
		kv.stateMechine = stateMechine
		kv.session = session
	}

	kv.lastApplyIndex = snapshotIndex

}

func (kv *KVServer) EncodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.stateMechine)
	e.Encode(kv.session)
	return w.Bytes()
}

func (kv *KVServer) takeSnapshot(index int) {
	data := kv.EncodeSnapshot()
	kv.rf.Snapshot(index, data)
}

func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate
}
