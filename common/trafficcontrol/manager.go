package trafficcontrol

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/compatible"
	"github.com/sagernet/sing/common/cleanup"
	"github.com/sagernet/sing/common/observable"
	"github.com/sagernet/sing/common/x/list"

	"github.com/gofrs/uuid/v5"
)

type ConnectionEventType int

const (
	ConnectionEventNew ConnectionEventType = iota
	ConnectionEventClosed
)

type ConnectionEvent struct {
	Type     ConnectionEventType
	ID       uuid.UUID
	Metadata *TrackerMetadata
	ClosedAt time.Time
}

const closedConnectionsLimit = 1000

var (
	_ adapter.ConnectionTracker = (*Manager)(nil)
	_ adapter.LifecycleService  = (*Manager)(nil)
)

type Manager struct {
	outbound adapter.OutboundManager

	// v2node 扩展：per-user 流量累计（连接关闭时结算，供 /users/traffic 端点）
	userUpload   sync.Map // user -> *atomic.Int64
	userDownload sync.Map // user -> *atomic.Int64

	connections             compatible.Map[uuid.UUID, Tracker]
	closedConnectionsAccess sync.Mutex
	closedConnections       list.List[TrackerMetadata]
	closedUploadTotal       int64
	closedDownloadTotal     int64

	eventSubscriber *observable.Subscriber[ConnectionEvent]
	eventObserver   *observable.Observer[ConnectionEvent]
	cleaner         *cleanup.Cleaner
}

// UserTrafficItem v2node 扩展：单个用户的流量累计。
type UserTrafficItem struct {
	User string `json:"user"`
	Up   int64  `json:"up"`
	Down int64  `json:"down"`
}

func NewManager(outbound adapter.OutboundManager) *Manager {
	return &Manager{
		outbound:        outbound,
		eventSubscriber: observable.NewSubscriber[ConnectionEvent](256),
	}
}

func (m *Manager) Name() string {
	return "traffic manager"
}

func (m *Manager) Start(stage adapter.StartStage) error {
	if stage == adapter.StartStateInitialize {
		m.eventObserver = observable.NewObserver(m.eventSubscriber, 64)
		m.cleaner = cleanup.Add(m.Clear)
	}
	return nil
}

func (m *Manager) Close() error {
	if m.cleaner != nil {
		m.cleaner.Close()
	}
	if m.eventObserver != nil {
		return m.eventObserver.Close()
	}
	return nil
}

func (m *Manager) SubscribeEvents() (observable.Subscription[ConnectionEvent], <-chan struct{}, error) {
	return m.eventObserver.Subscribe()
}

func (m *Manager) UnSubscribeEvents(subscription observable.Subscription[ConnectionEvent]) {
	m.eventObserver.UnSubscribe(subscription)
}

func (m *Manager) join(tracker Tracker) {
	metadata := tracker.Metadata()
	m.connections.Store(metadata.ID, tracker)
	m.eventSubscriber.Emit(ConnectionEvent{
		Type:     ConnectionEventNew,
		ID:       metadata.ID,
		Metadata: metadata,
	})
}

func (m *Manager) leave(tracker Tracker) {
	metadata := tracker.Metadata()
	closedAt := time.Now()
	m.closedConnectionsAccess.Lock()
	_, loaded := m.connections.LoadAndDelete(metadata.ID)
	if !loaded {
		m.closedConnectionsAccess.Unlock()
		return
	}
	// v2node 扩展：连接关闭时把最终流量结算到 per-user 累计。
	if metadata.Metadata.User != "" {
		up, _ := m.userUpload.LoadOrStore(metadata.Metadata.User, new(atomic.Int64))
		up.(*atomic.Int64).Add(metadata.Upload.Load())
		down, _ := m.userDownload.LoadOrStore(metadata.Metadata.User, new(atomic.Int64))
		down.(*atomic.Int64).Add(metadata.Download.Load())
	}
	metadata.ClosedAt = closedAt
	metadataCopy := *metadata
	if m.closedConnections.Len() >= closedConnectionsLimit {
		evicted := m.closedConnections.PopFront()
		m.closedUploadTotal += evicted.Upload.Load()
		m.closedDownloadTotal += evicted.Download.Load()
	}
	m.closedConnections.PushBack(metadataCopy)
	m.closedConnectionsAccess.Unlock()
	m.eventSubscriber.Emit(ConnectionEvent{
		Type:     ConnectionEventClosed,
		ID:       metadata.ID,
		Metadata: &metadataCopy,
		ClosedAt: closedAt,
	})
}

func (m *Manager) Total() (uplinkTotal int64, downlinkTotal int64) {
	m.closedConnectionsAccess.Lock()
	defer m.closedConnectionsAccess.Unlock()
	uplinkTotal = m.closedUploadTotal
	downlinkTotal = m.closedDownloadTotal
	for element := m.closedConnections.Front(); element != nil; element = element.Next() {
		uplinkTotal += element.Value.Upload.Load()
		downlinkTotal += element.Value.Download.Load()
	}
	m.connections.Range(func(_ uuid.UUID, tracker Tracker) bool {
		metadata := tracker.Metadata()
		uplinkTotal += metadata.Upload.Load()
		downlinkTotal += metadata.Download.Load()
		return true
	})
	return
}

// UserTraffic v2node 扩展：per-user 流量累计（已关闭连接的结算值 + 活跃连接的实时值）。
// 返回值为单调递增的进程内累计，v2node 采集侧做差量即可。
func (m *Manager) UserTraffic() []UserTrafficItem {
	totals := make(map[string]*UserTrafficItem)
	get := func(user string) *UserTrafficItem {
		item, ok := totals[user]
		if !ok {
			item = &UserTrafficItem{User: user}
			totals[user] = item
		}
		return item
	}
	m.userUpload.Range(func(key, value any) bool {
		get(key.(string)).Up += value.(*atomic.Int64).Load()
		return true
	})
	m.userDownload.Range(func(key, value any) bool {
		get(key.(string)).Down += value.(*atomic.Int64).Load()
		return true
	})
	// 活跃连接：临时并入实时值（不写入累计，避免连接关闭时重复结算）。
	m.connections.Range(func(_ uuid.UUID, tracker Tracker) bool {
		metadata := tracker.Metadata()
		if metadata.Metadata.User == "" {
			return true
		}
		item := get(metadata.Metadata.User)
		item.Up += metadata.Upload.Load()
		item.Down += metadata.Download.Load()
		return true
	})
	items := make([]UserTrafficItem, 0, len(totals))
	for _, item := range totals {
		items = append(items, *item)
	}
	return items
}

func (m *Manager) ConnectionsLen() int {
	return m.connections.Len()
}

func (m *Manager) Connections() []*TrackerMetadata {
	var connections []*TrackerMetadata
	m.connections.Range(func(_ uuid.UUID, tracker Tracker) bool {
		connections = append(connections, tracker.Metadata())
		return true
	})
	return connections
}

func (m *Manager) ClosedConnections() []*TrackerMetadata {
	m.closedConnectionsAccess.Lock()
	values := m.closedConnections.Array()
	m.closedConnectionsAccess.Unlock()
	if len(values) == 0 {
		return nil
	}
	connections := make([]*TrackerMetadata, len(values))
	for i := range values {
		connections[i] = &values[i]
	}
	return connections
}

func (m *Manager) Connection(id uuid.UUID) Tracker {
	connection, loaded := m.connections.Load(id)
	if !loaded {
		return nil
	}
	return connection
}

func (m *Manager) CloseAllConnections() {
	m.connections.Range(func(_ uuid.UUID, tracker Tracker) bool {
		tracker.Close()
		return true
	})
}

func (m *Manager) Clear() {
	m.closedConnectionsAccess.Lock()
	defer m.closedConnectionsAccess.Unlock()
	for element := m.closedConnections.Front(); element != nil; element = element.Next() {
		m.closedUploadTotal += element.Value.Upload.Load()
		m.closedDownloadTotal += element.Value.Download.Load()
	}
	m.closedConnections.Init()
}
