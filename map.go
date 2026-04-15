package node

import (
	"context"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// Version is a physical-time LWW version for one key.
// Ordering is lexicographic: (Timestamp, NodeID).
type Version struct {
	Timestamp uint64
	NodeID    string
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode

	mu sync.RWMutex

	allNodeIDs    []string
	state         MapState
	lastTimestamp uint64
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	nodeIDsCopy := append([]string(nil), allNodeIDs...)

	return &CRDTMapNode{
		BaseNode:   hive.NewBaseNode(id),
		allNodeIDs: nodeIDsCopy,
		state:      make(MapState),
	}
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	n.BaseNode.Start(ctx)

	go func() {
		ticker := time.NewTicker(228 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n.broadcastState()
			}
		}
	}()

	return nil
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state[k] = StateEntry{
		Value:     v,
		Tombstone: false,
		Version:   n.nextVersionLocked(),
	}
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	entry, ok := n.state[k]
	if !ok || entry.Tombstone {
		return "", false
	}

	return entry.Value, true
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state[k] = StateEntry{
		Tombstone: true,
		Version:   n.nextVersionLocked(),
	}
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for k, remoteEntry := range remote {
		localEntry, exists := n.state[k]
		if !exists || isNewer(remoteEntry.Version, localEntry.Version) {
			n.state[k] = remoteEntry
		}
	}
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.RLock()
	defer n.mu.RUnlock()

	out := make(MapState, len(n.state))
	for k, entry := range n.state {
		out[k] = entry
	}

	return out
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	out := make(map[string]string)
	for k, entry := range n.state {
		if entry.Tombstone {
			continue
		}
		out[k] = entry.Value
	}

	return out
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	switch payload := msg.Payload.(type) {
	case MapState:
		n.Merge(payload)
	}

	return nil
}

func (n *CRDTMapNode) nextVersionLocked() Version {
	now := (uint64)(time.Now().UnixNano())
	if now <= n.lastTimestamp {
		now = n.lastTimestamp + 1
	}
	n.lastTimestamp = now

	return Version{
		Timestamp: n.lastTimestamp,
		NodeID:    n.ID(),
	}
}

func (n *CRDTMapNode) broadcastState() {
	state := n.State()

	for _, to := range n.allNodeIDs {
		if to == n.ID() {
			continue
		}
		_ = n.SendMessage(hive.NewMessage(n.ID(), to, state))
	}
}

func isNewer(a, b Version) bool {
	if a.Timestamp != b.Timestamp {
		return a.Timestamp > b.Timestamp
	}
	return a.NodeID > b.NodeID
}
