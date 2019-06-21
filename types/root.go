package types

//Root forms a base on top of which a participant's Events can be inserted. It
//contains FrameEvents sorted by Lamport timestamp.
type Root struct {
	Events []*FrameEvent
}

//NewRoot instantianted an new empty root
//TODO: This should be a genesis root, this should have full data
func NewRoot() *Root {
	return &Root{
		Events: []*FrameEvent{},
	}
}

//Insert appends a FrameEvent to the root's Event slice. It is assumend that
//items are inserted in topological order.
func (r *Root) Insert(frameEvent *FrameEvent) {
	r.Events = append(r.Events, frameEvent)
}
