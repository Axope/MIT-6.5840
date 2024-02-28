package raft

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type LogEntries struct {
	Entries []Entry
}

func NewLogEntries() LogEntries {
	return LogEntries{
		Entries: make([]Entry, 1),
	}
}

func (log *LogEntries) getLastLog() Entry {
	return log.Entries[len(log.Entries)-1]
}

func (log *LogEntries) getFirstLog() Entry {
	return log.Entries[0]
}

func (log *LogEntries) getLogEntryByIndex(index int) Entry {
	firstIndex := log.getFirstLog().Index
	return log.Entries[index-firstIndex]
}

func (log *LogEntries) getSuffixEntries(index int) []Entry {
	firstIndex := log.getFirstLog().Index
	return append([]Entry(nil), log.Entries[index-firstIndex:]...)
}

func (log *LogEntries) getPrefixEntries(index int) []Entry {
	firstIndex := log.getFirstLog().Index
	return append([]Entry(nil), log.Entries[:index-firstIndex+1]...)
}

func (log *LogEntries) getRangeEntries(indexL, indexR int) []Entry {
	firstIndex := log.getFirstLog().Index
	res := make([]Entry, indexR-indexL+1)
	copy(res, log.Entries[indexL-firstIndex:indexR-firstIndex+1])
	return res
}

func (log *LogEntries) append(e []Entry) {
	log.Entries = append(log.Entries, e...)
}

// 保证index合法
func (log *LogEntries) findFirstConflict(index int) (int, int) {
	res := index
	firstIndex := log.getFirstLog().Index
	arrayIndex := res - firstIndex
	tmpTerm := log.Entries[arrayIndex].Term

	for arrayIndex-1 > 0 && log.Entries[arrayIndex-1].Term == tmpTerm {
		arrayIndex--
		res--
	}
	return tmpTerm, res
}

func (log *LogEntries) setFirstLog(e Entry) {
	log.Entries[0] = e
}
