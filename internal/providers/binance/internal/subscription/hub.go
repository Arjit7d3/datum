package subscription

import (
	"context"
	"errors"
)

type hub struct {
	in     chan []byte
	add    chan chan []byte
	del    chan chan []byte
	subs   map[chan []byte]struct{}
	cancel context.CancelFunc
}

func newHub(ctx context.Context, cancel context.CancelFunc) *hub {
	h := &hub{
		in:     make(chan []byte),
		add:    make(chan chan []byte),
		del:    make(chan chan []byte),
		subs:   make(map[chan []byte]struct{}),
		cancel: cancel,
	}
	go h.run(ctx)
	return h
}

func (h *hub) run(ctx context.Context) {
	defer func() {
		for sub := range h.subs {
			close(sub)
		}
		h.subs = nil
	}()

	for {
		select {
		case sub := <-h.add:
			h.subs[sub] = struct{}{}
		case sub := <-h.del:
			if _, ok := h.subs[sub]; ok {
				close(sub)
				delete(h.subs, sub)
			}
		case msg := <-h.in:
			for ch := range h.subs {
				select {
				case ch <- msg:
				default:
					// Skip slow consumers
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// subscribe returns a bidirectional channel so it can be passed back to unsubscribe
func (h *hub) subscribe(ctx context.Context) (chan []byte, error) {
	ch := make(chan []byte, 100)

	select {
	case h.add <- ch:
		return ch, nil
	case <-ctx.Done():
		return nil, errors.New("hub is dead")
	}
}

func (h *hub) unsubscribe(ch chan []byte) {
	// Unsubscribe is best-effort
	select {
	case h.del <- ch:
	default:
	}
}

func (h *hub) stop() {
	if h.cancel != nil {
		h.cancel()
	}
}
