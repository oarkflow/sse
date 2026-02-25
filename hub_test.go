package sse

import (
	"testing"
	"time"
)

func newTestHub() *Hub {
	return NewHub(HubOptions{
		StickySize:        5,
		ClientBufferSize:  16,
		HeartbeatInterval: 0, // disable for tests
	})
}

func newTestClient(id string) *Client {
	return NewClient(ClientOptions{ID: id, BufferSize: 16})
}

func recv(t *testing.T, c *Client, timeout time.Duration) *Event {
	t.Helper()
	select {
	case e := <-c.Recv():
		return e
	case <-time.After(timeout):
		t.Fatalf("client %s: timed out waiting for event", c.ID)
		return nil
	}
}

func TestBroadcast(t *testing.T) {
	hub := newTestHub()
	defer hub.Stop()

	c1 := newTestClient("c1")
	c2 := newTestClient("c2")
	_ = hub.AddClient(c1)
	_ = hub.AddClient(c2)
	time.Sleep(10 * time.Millisecond)

	hub.Broadcast(NewEvent("test", "hello"))

	for _, c := range []*Client{c1, c2} {
		e := recv(t, c, 200*time.Millisecond)
		if e.Data != "hello" {
			t.Errorf("expected 'hello', got %q", e.Data)
		}
	}
}

func TestSendToClient(t *testing.T) {
	hub := newTestHub()
	defer hub.Stop()

	c1 := newTestClient("c1")
	c2 := newTestClient("c2")
	_ = hub.AddClient(c1)
	_ = hub.AddClient(c2)
	time.Sleep(10 * time.Millisecond)

	_ = hub.Send("c1", NewEvent("direct", "for c1 only"))

	e := recv(t, c1, 200*time.Millisecond)
	if e.Type != "direct" {
		t.Errorf("wrong event type: %s", e.Type)
	}

	// c2 should NOT receive anything
	select {
	case <-c2.Recv():
		t.Fatal("c2 received an event it shouldn't have")
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

func TestSendToClientNotFound(t *testing.T) {
	hub := newTestHub()
	defer hub.Stop()

	err := hub.Send("ghost", NewEvent("ping", ""))
	if err != ErrClientNotFound {
		t.Errorf("expected ErrClientNotFound, got %v", err)
	}
}

func TestGroups(t *testing.T) {
	hub := newTestHub()
	defer hub.Stop()

	c1 := newTestClient("c1")
	c2 := newTestClient("c2")
	c3 := newTestClient("c3")
	_ = hub.AddClient(c1)
	_ = hub.AddClient(c2)
	_ = hub.AddClient(c3)
	time.Sleep(10 * time.Millisecond)

	_ = hub.JoinGroup("c1", "room-a")
	_ = hub.JoinGroup("c2", "room-a")

	hub.SendToGroup("room-a", NewEvent("room", "hi room-a"))

	for _, c := range []*Client{c1, c2} {
		e := recv(t, c, 200*time.Millisecond)
		if e.Data != "hi room-a" {
			t.Errorf("client %s: wrong data: %s", c.ID, e.Data)
		}
	}

	// c3 must not receive the group message
	select {
	case <-c3.Recv():
		t.Fatal("c3 received group event unexpectedly")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestLeaveGroup(t *testing.T) {
	hub := newTestHub()
	defer hub.Stop()

	c1 := newTestClient("c1")
	_ = hub.AddClient(c1)
	time.Sleep(10 * time.Millisecond)

	_ = hub.JoinGroup("c1", "room-b")
	hub.LeaveGroup("c1", "room-b")
	hub.SendToGroup("room-b", NewEvent("msg", "anyone there?"))

	select {
	case <-c1.Recv():
		t.Fatal("c1 received event after leaving group")
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

func TestBroadcastExcept(t *testing.T) {
	hub := newTestHub()
	defer hub.Stop()

	c1 := newTestClient("c1")
	c2 := newTestClient("c2")
	c3 := newTestClient("c3")
	_ = hub.AddClient(c1)
	_ = hub.AddClient(c2)
	_ = hub.AddClient(c3)
	time.Sleep(10 * time.Millisecond)

	hub.BroadcastExcept(NewEvent("msg", "not c2"), "c2")

	recv(t, c1, 200*time.Millisecond)
	recv(t, c3, 200*time.Millisecond)

	select {
	case <-c2.Recv():
		t.Fatal("c2 should have been excluded")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestStickyReplay(t *testing.T) {
	hub := newTestHub()
	defer hub.Stop()

	// Publish before client connects.
	hub.Broadcast(NewEvent("update", "state-1").WithTopic("state"))
	hub.Broadcast(NewEvent("update", "state-2").WithTopic("state"))
	time.Sleep(10 * time.Millisecond)

	late := newTestClient("late")
	_ = hub.AddClient(late)

	received := map[string]bool{}
	for i := 0; i < 2; i++ {
		e := recv(t, late, 500*time.Millisecond)
		received[e.Data] = true
	}
	if !received["state-1"] || !received["state-2"] {
		t.Errorf("sticky replay incomplete: got %v", received)
	}
}

func TestStickyRingBuffer(t *testing.T) {
	hub := NewHub(HubOptions{StickySize: 3, HeartbeatInterval: 0})
	defer hub.Stop()

	for _, d := range []string{"e1", "e2", "e3", "e4", "e5"} {
		hub.Broadcast(NewEvent("update", d).WithTopic("nums"))
	}
	time.Sleep(10 * time.Millisecond)

	late := newTestClient("late")
	_ = hub.AddClient(late)

	received := []string{}
	for i := 0; i < 3; i++ {
		e := recv(t, late, 500*time.Millisecond)
		received = append(received, e.Data)
	}

	// Should not get a 4th event
	select {
	case e := <-late.Recv():
		t.Fatalf("got unexpected 4th replayed event: %+v", e)
	case <-time.After(50 * time.Millisecond):
	}

	t.Logf("replayed events: %v (expect last 3 of 5)", received)
}

func TestMaxClients(t *testing.T) {
	hub := NewHub(HubOptions{MaxClients: 2, HeartbeatInterval: 0})
	defer hub.Stop()

	_ = hub.AddClient(newTestClient("c1"))
	_ = hub.AddClient(newTestClient("c2"))
	time.Sleep(10 * time.Millisecond)

	err := hub.AddClient(newTestClient("c3"))
	if err != ErrMaxClientsReached {
		t.Errorf("expected ErrMaxClientsReached, got %v", err)
	}
}

func TestStats(t *testing.T) {
	hub := newTestHub()
	defer hub.Stop()

	c1 := newTestClient("c1")
	c2 := newTestClient("c2")
	_ = hub.AddClient(c1)
	_ = hub.AddClient(c2)
	time.Sleep(10 * time.Millisecond)
	_ = hub.JoinGroup("c1", "room-z")
	_ = hub.JoinGroup("c2", "room-z")

	s := hub.Stats()
	if s.ConnectedClients != 2 {
		t.Errorf("expected 2 clients, got %d", s.ConnectedClients)
	}
	if s.Groups["room-z"] != 2 {
		t.Errorf("expected 2 members in room-z, got %d", s.Groups["room-z"])
	}
}

func TestStopClosesClientChannels(t *testing.T) {
	hub := newTestHub()
	c := newTestClient("c1")
	_ = hub.AddClient(c)
	time.Sleep(10 * time.Millisecond)

	hub.Stop()

	select {
	case _, ok := <-c.Recv():
		if ok {
			t.Fatal("expected client channel to be closed after Stop")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for client channel to close after Stop")
	}
}

func TestAddClientAfterStop(t *testing.T) {
	hub := newTestHub()
	hub.Stop()
	time.Sleep(10 * time.Millisecond)

	err := hub.AddClient(newTestClient("after-stop"))
	if err != ErrHubClosed {
		t.Errorf("expected ErrHubClosed, got %v", err)
	}
}

func TestRemoveClientAfterStopDoesNotBlock(t *testing.T) {
	hub := newTestHub()
	c := newTestClient("c1")
	_ = hub.AddClient(c)
	time.Sleep(10 * time.Millisecond)
	hub.Stop()

	done := make(chan struct{})
	go func() {
		hub.RemoveClient(c)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("RemoveClient blocked after hub Stop")
	}
}

func TestEventEncode(t *testing.T) {
	e := &Event{
		ID:   "123",
		Type: "update",
		Data: "line1\nline2",
	}
	encoded := string(e.Encode())
	expected := "id: 123\nevent: update\ndata: line1\ndata: line2\n\n"
	if encoded != expected {
		t.Errorf("encode mismatch:\ngot:  %q\nwant: %q", encoded, expected)
	}
}

func TestAddClientWhileDraining(t *testing.T) {
	hub := newTestHub()
	defer hub.Stop()
	hub.StartDraining()

	err := hub.AddClient(newTestClient("drain"))
	if err != ErrHubDraining {
		t.Fatalf("expected ErrHubDraining, got %v", err)
	}
}

func TestMaxGroupsPerClient(t *testing.T) {
	hub := NewHub(HubOptions{
		MaxGroupsPerClient: 2,
		HeartbeatInterval:  0,
	})
	defer hub.Stop()

	c := newTestClient("c1")
	_ = hub.AddClient(c)
	time.Sleep(10 * time.Millisecond)

	if err := hub.JoinGroup("c1", "g1"); err != nil {
		t.Fatalf("unexpected join error: %v", err)
	}
	if err := hub.JoinGroup("c1", "g2"); err != nil {
		t.Fatalf("unexpected join error: %v", err)
	}
	if err := hub.JoinGroup("c1", "g3"); err != ErrClientGroupLimitExceeded {
		t.Fatalf("expected ErrClientGroupLimitExceeded, got %v", err)
	}
}

func TestReplayAfterLastEventID(t *testing.T) {
	hub := NewHub(HubOptions{
		HeartbeatInterval:     0,
		ReplayBufferSize:      10,
		ReplayLimitPerConnect: 10,
	})
	defer hub.Stop()

	e1 := NewEvent("update", "one").WithTopic("sync")
	e2 := NewEvent("update", "two").WithTopic("sync")
	e3 := NewEvent("update", "three").WithTopic("sync")
	hub.Broadcast(e1)
	hub.Broadcast(e2)
	hub.Broadcast(e3)

	late := NewClient(ClientOptions{
		ID:          "late",
		LastEventID: e1.ID,
		Topics:      []string{"sync"},
		BufferSize:  16,
	})
	_ = hub.AddClient(late)

	gotA := recv(t, late, 500*time.Millisecond)
	gotB := recv(t, late, 500*time.Millisecond)
	if gotA.Data != "two" || gotB.Data != "three" {
		t.Fatalf("expected replay two/three, got %q/%q", gotA.Data, gotB.Data)
	}
}

func TestEventValidationLimits(t *testing.T) {
	hub := NewHub(HubOptions{
		HeartbeatInterval:  0,
		MaxEventBytes:      4,
		MaxEventTypeLength: 3,
	})
	defer hub.Stop()

	if err := hub.ValidateEvent(NewEvent("toolong", "ok")); err != ErrInvalidEvent {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
	if err := hub.ValidateEvent(NewEvent("ok", "12345")); err != ErrEventTooLarge {
		t.Fatalf("expected ErrEventTooLarge, got %v", err)
	}
}
