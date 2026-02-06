package subscription

import "sync"

type hub struct {
	in   chan []byte
	add  chan chan []byte
	del  chan chan []byte
	stop chan struct{}

	subs map[chan []byte]struct{}
	mu   sync.Mutex // Protects subs map for external access (Unsubscribe)
}

func newHub() *hub {
	h := &hub{
		in:   make(chan []byte),
		add:  make(chan chan []byte),
		del:  make(chan chan []byte),
		stop: make(chan struct{}),
		subs: make(map[chan []byte]struct{}),
	}
	go h.run()
	return h
}

func (h *hub) run() {
	for {
		select {
		case sub := <-h.add:
			h.mu.Lock()
			h.subs[sub] = struct{}{}
			h.mu.Unlock()
		case sub := <-h.del:
			h.mu.Lock()
			if _, ok := h.subs[sub]; ok {
				close(sub)
				delete(h.subs, sub)
			}
			h.mu.Unlock()
		case msg := <-h.in:
			h.mu.Lock()
			for ch := range h.subs {
				select {
				case ch <- msg:
				default:
					// Skip slow consumers
				}
			}
			h.mu.Unlock()
		case <-h.stop:
			h.mu.Lock()
			for sub := range h.subs {
				close(sub)
				delete(h.subs, sub)
			}
			h.mu.Unlock()
			return
		}
	}
}

func (h *hub) subscribe() <-chan []byte {
	ch := make(chan []byte, 100) // Added buffer to reduce dropped messages
	h.add <- ch
	return ch
}

func (h *hub) unsubscribe(ch chan []byte) {
	h.del <- ch
}

func (h *hub) stopHub() {
	select {
	case h.stop <- struct{}{}:
	default:
		// already stopping
	}
}
