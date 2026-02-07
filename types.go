package datum

import "github.com/Arjit7d3/datum/internal/core"

// StreamRequest defines a request for a data stream
type StreamRequest[T any] interface {
	CreateStream(core.Provider) core.IStream[T]
}

// QueryRequest defines a request for a data query
type QueryRequest[T any] interface {
	CreateQuery(core.Provider) core.IQuery[T]
}
