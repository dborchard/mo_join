package colexec

import (
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"reflect"
)

type ResultPos struct {
	Rel int32
	Pos int32
}

// ReceiverOperator need to receive batch from proc.Reg.MergeReceivers
type ReceiverOperator struct {
	proc *process.Process

	// parameter for Merge-Type receiver.
	// Merge-Type specifys the operator receive batch from all
	// regs or single reg.
	//
	// Merge/MergeGroup/MergeLimit ... are Merge-Type
	// while Join/Intersect/Minus ... are not
	aliveMergeReceiver int
	chs                []chan *batch.Batch
	receiverListener   []reflect.SelectCase
}

const (
	DefaultBatchSize = 8192
)
