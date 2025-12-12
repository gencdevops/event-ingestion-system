package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/event-ingestion/domain/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockConn is a mock implementation of driver.Conn
type MockConn struct {
	mock.Mock
}

func (m *MockConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(driver.Batch), args.Error(1)
}

func (m *MockConn) QueryRow(ctx context.Context, query string, queryArgs ...any) driver.Row {
	args := m.Called(ctx, query, queryArgs)
	return args.Get(0).(driver.Row)
}

func (m *MockConn) Query(ctx context.Context, query string, queryArgs ...any) (driver.Rows, error) {
	args := m.Called(ctx, query, queryArgs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(driver.Rows), args.Error(1)
}

func (m *MockConn) Exec(ctx context.Context, query string, queryArgs ...any) error {
	args := m.Called(ctx, query, queryArgs)
	return args.Error(0)
}

func (m *MockConn) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockConn) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockConn) Stats() driver.Stats {
	args := m.Called()
	return args.Get(0).(driver.Stats)
}

func (m *MockConn) AsyncInsert(ctx context.Context, query string, wait bool, queryArgs ...any) error {
	args := m.Called(ctx, query, wait, queryArgs)
	return args.Error(0)
}

func (m *MockConn) Select(ctx context.Context, dest any, query string, queryArgs ...any) error {
	args := m.Called(ctx, dest, query, queryArgs)
	return args.Error(0)
}

func (m *MockConn) Contributors() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockConn) ServerVersion() (*driver.ServerVersion, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*driver.ServerVersion), args.Error(1)
}

// MockBatch is a mock implementation of driver.Batch
type MockBatch struct {
	mock.Mock
	appendCount int
}

func (m *MockBatch) Append(v ...any) error {
	m.appendCount++
	args := m.Called(v)
	return args.Error(0)
}

func (m *MockBatch) AppendStruct(v any) error {
	args := m.Called(v)
	return args.Error(0)
}

func (m *MockBatch) Column(idx int) driver.BatchColumn {
	args := m.Called(idx)
	return args.Get(0).(driver.BatchColumn)
}

func (m *MockBatch) Abort() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockBatch) Flush() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockBatch) Send() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockBatch) IsSent() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockBatch) Rows() int {
	return m.appendCount
}

func (m *MockBatch) Columns() []column.Interface {
	return nil
}

// MockRow is a mock implementation of driver.Row
type MockRow struct {
	mock.Mock
	scanFunc func(dest ...any) error
}

func (m *MockRow) Scan(dest ...any) error {
	if m.scanFunc != nil {
		return m.scanFunc(dest...)
	}
	args := m.Called(dest)
	return args.Error(0)
}

func (m *MockRow) ScanStruct(dest any) error {
	args := m.Called(dest)
	return args.Error(0)
}

func (m *MockRow) Err() error {
	args := m.Called()
	return args.Error(0)
}

// MockRows is a mock implementation of driver.Rows
type MockRows struct {
	mock.Mock
	currentIndex int
	data         [][]any
	columns      []string
	scanErr      error
	errVal       error
}

func NewMockRows(data [][]any, columns []string) *MockRows {
	return &MockRows{
		currentIndex: -1,
		data:         data,
		columns:      columns,
	}
}

func (m *MockRows) Next() bool {
	m.currentIndex++
	return m.currentIndex < len(m.data)
}

func (m *MockRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if m.currentIndex >= len(m.data) {
		return nil
	}
	row := m.data[m.currentIndex]
	for i, v := range row {
		if i < len(dest) {
			switch d := dest[i].(type) {
			case *string:
				*d = v.(string)
			case *uint64:
				*d = v.(uint64)
			case *int64:
				*d = v.(int64)
			}
		}
	}
	return nil
}

func (m *MockRows) ScanStruct(dest any) error {
	args := m.Called(dest)
	return args.Error(0)
}

func (m *MockRows) Close() error {
	return nil
}

func (m *MockRows) Err() error {
	return m.errVal
}

func (m *MockRows) Columns() []string {
	return m.columns
}

func (m *MockRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (m *MockRows) Totals(dest ...any) error {
	args := m.Called(dest)
	return args.Error(0)
}

// testableRepository wraps repository for testing
type testableRepository struct {
	conn     driver.Conn
	database string
}

func newTestableRepository(conn driver.Conn, database string) *testableRepository {
	return &testableRepository{conn: conn, database: database}
}

func createTestEvents(count int) []*event.Event {
	events := make([]*event.Event, count)
	for i := 0; i < count; i++ {
		events[i] = &event.Event{
			EventID:   "event-" + string(rune('a'+i)),
			EventName: "test_event",
			UserID:    "user-123",
			Channel:   "web",
			Timestamp: time.Now().Unix(),
			Tags:      []string{"tag1", "tag2"},
			Metadata:  map[string]interface{}{"key": "value"},
		}
	}
	return events
}

func TestInsertBatch_EmptyEvents(t *testing.T) {
	mockConn := new(MockConn)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	ctx := context.Background()
	err := repo.InsertBatch(ctx, []*event.Event{})

	require.NoError(t, err)
	mockConn.AssertNotCalled(t, "PrepareBatch")
}

func TestInsertBatch_SingleEvent(t *testing.T) {
	mockConn := new(MockConn)
	mockBatch := new(MockBatch)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	events := createTestEvents(1)
	ctx := context.Background()

	mockConn.On("PrepareBatch", ctx, mock.AnythingOfType("string")).Return(mockBatch, nil)
	mockBatch.On("Append", mock.Anything).Return(nil)
	mockBatch.On("Send").Return(nil)

	err := repo.InsertBatch(ctx, events)

	require.NoError(t, err)
	mockConn.AssertExpectations(t)
	mockBatch.AssertExpectations(t)
}

func TestInsertBatch_MultipleEvents(t *testing.T) {
	mockConn := new(MockConn)
	mockBatch := new(MockBatch)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	events := createTestEvents(5)
	ctx := context.Background()

	mockConn.On("PrepareBatch", ctx, mock.AnythingOfType("string")).Return(mockBatch, nil)
	mockBatch.On("Append", mock.Anything).Return(nil)
	mockBatch.On("Send").Return(nil)

	err := repo.InsertBatch(ctx, events)

	require.NoError(t, err)
	mockConn.AssertExpectations(t)
	assert.Equal(t, 5, mockBatch.appendCount)
}

func TestInsertBatch_PrepareBatchError(t *testing.T) {
	mockConn := new(MockConn)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	events := createTestEvents(1)
	ctx := context.Background()

	mockConn.On("PrepareBatch", ctx, mock.AnythingOfType("string")).Return(nil, errors.New("prepare failed"))

	err := repo.InsertBatch(ctx, events)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to prepare batch")
}

func TestInsertBatch_AppendError(t *testing.T) {
	mockConn := new(MockConn)
	mockBatch := new(MockBatch)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	events := createTestEvents(1)
	ctx := context.Background()

	mockConn.On("PrepareBatch", ctx, mock.AnythingOfType("string")).Return(mockBatch, nil)
	mockBatch.On("Append", mock.Anything).Return(errors.New("append failed"))

	err := repo.InsertBatch(ctx, events)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to append to batch")
}

func TestInsertBatch_SendError(t *testing.T) {
	mockConn := new(MockConn)
	mockBatch := new(MockBatch)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	events := createTestEvents(1)
	ctx := context.Background()

	mockConn.On("PrepareBatch", ctx, mock.AnythingOfType("string")).Return(mockBatch, nil)
	mockBatch.On("Append", mock.Anything).Return(nil)
	mockBatch.On("Send").Return(errors.New("send failed"))

	err := repo.InsertBatch(ctx, events)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to send batch")
}

func TestGetMetrics_Success(t *testing.T) {
	mockConn := new(MockConn)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	query := &event.GetMetricsQuery{
		EventName: "test_event",
		From:      time.Now().Add(-24 * time.Hour).Unix(),
		To:        time.Now().Unix(),
	}
	ctx := context.Background()

	mockRow := &MockRow{
		scanFunc: func(dest ...any) error {
			if len(dest) >= 2 {
				*dest[0].(*uint64) = 100
				*dest[1].(*uint64) = 50
			}
			return nil
		},
	}

	mockConn.On("QueryRow", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRow)

	result, err := repo.GetMetrics(ctx, query)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(100), result.TotalCount)
	assert.Equal(t, int64(50), result.UniqueUsers)
}

func TestGetMetrics_WithChannelFilter(t *testing.T) {
	mockConn := new(MockConn)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	query := &event.GetMetricsQuery{
		EventName: "test_event",
		From:      time.Now().Add(-24 * time.Hour).Unix(),
		To:        time.Now().Unix(),
		Channel:   "web",
	}
	ctx := context.Background()

	mockRow := &MockRow{
		scanFunc: func(dest ...any) error {
			if len(dest) >= 2 {
				*dest[0].(*uint64) = 75
				*dest[1].(*uint64) = 30
			}
			return nil
		},
	}

	mockConn.On("QueryRow", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRow)

	result, err := repo.GetMetrics(ctx, query)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(75), result.TotalCount)
	assert.Equal(t, int64(30), result.UniqueUsers)
}

func TestGetMetrics_ScanError(t *testing.T) {
	mockConn := new(MockConn)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	query := &event.GetMetricsQuery{
		EventName: "test_event",
		From:      time.Now().Add(-24 * time.Hour).Unix(),
		To:        time.Now().Unix(),
	}
	ctx := context.Background()

	mockRow := &MockRow{
		scanFunc: func(dest ...any) error {
			return errors.New("scan failed")
		},
	}

	mockConn.On("QueryRow", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRow)

	result, err := repo.GetMetrics(ctx, query)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get metrics")
}

func TestGetMetrics_WithGroupByChannel(t *testing.T) {
	mockConn := new(MockConn)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	query := &event.GetMetricsQuery{
		EventName: "test_event",
		From:      time.Now().Add(-24 * time.Hour).Unix(),
		To:        time.Now().Unix(),
		GroupBy:   "channel",
	}
	ctx := context.Background()

	mockRow := &MockRow{
		scanFunc: func(dest ...any) error {
			if len(dest) >= 2 {
				*dest[0].(*uint64) = 100
				*dest[1].(*uint64) = 50
			}
			return nil
		},
	}

	mockRows := NewMockRows([][]any{
		{"web", uint64(60), uint64(30)},
		{"mobile_app", uint64(40), uint64(20)},
	}, []string{"key", "total_count", "unique_users"})

	mockConn.On("QueryRow", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRow)
	mockConn.On("Query", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRows, nil)

	result, err := repo.GetMetrics(ctx, query)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(100), result.TotalCount)
	assert.Len(t, result.GroupedData, 2)
	assert.Equal(t, "web", result.GroupedData[0].Key)
	assert.Equal(t, int64(60), result.GroupedData[0].TotalCount)
}

func TestGetMetrics_WithGroupByHour(t *testing.T) {
	mockConn := new(MockConn)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	query := &event.GetMetricsQuery{
		EventName: "test_event",
		From:      time.Now().Add(-24 * time.Hour).Unix(),
		To:        time.Now().Unix(),
		GroupBy:   "hour",
	}
	ctx := context.Background()

	mockRow := &MockRow{
		scanFunc: func(dest ...any) error {
			if len(dest) >= 2 {
				*dest[0].(*uint64) = 100
				*dest[1].(*uint64) = 50
			}
			return nil
		},
	}

	mockRows := NewMockRows([][]any{
		{"2024-01-01 10:00:00", uint64(30), uint64(15)},
		{"2024-01-01 11:00:00", uint64(70), uint64(35)},
	}, []string{"key", "total_count", "unique_users"})

	mockConn.On("QueryRow", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRow)
	mockConn.On("Query", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRows, nil)

	result, err := repo.GetMetrics(ctx, query)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.GroupedData, 2)
}

func TestGetMetrics_WithGroupByDay(t *testing.T) {
	mockConn := new(MockConn)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	query := &event.GetMetricsQuery{
		EventName: "test_event",
		From:      time.Now().Add(-7 * 24 * time.Hour).Unix(),
		To:        time.Now().Unix(),
		GroupBy:   "day",
	}
	ctx := context.Background()

	mockRow := &MockRow{
		scanFunc: func(dest ...any) error {
			if len(dest) >= 2 {
				*dest[0].(*uint64) = 500
				*dest[1].(*uint64) = 200
			}
			return nil
		},
	}

	mockRows := NewMockRows([][]any{
		{"2024-01-01", uint64(100), uint64(50)},
		{"2024-01-02", uint64(150), uint64(60)},
		{"2024-01-03", uint64(250), uint64(90)},
	}, []string{"key", "total_count", "unique_users"})

	mockConn.On("QueryRow", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRow)
	mockConn.On("Query", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRows, nil)

	result, err := repo.GetMetrics(ctx, query)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.GroupedData, 3)
}

func TestGetMetrics_GroupedQueryError(t *testing.T) {
	mockConn := new(MockConn)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	query := &event.GetMetricsQuery{
		EventName: "test_event",
		From:      time.Now().Add(-24 * time.Hour).Unix(),
		To:        time.Now().Unix(),
		GroupBy:   "channel",
	}
	ctx := context.Background()

	mockRow := &MockRow{
		scanFunc: func(dest ...any) error {
			if len(dest) >= 2 {
				*dest[0].(*uint64) = 100
				*dest[1].(*uint64) = 50
			}
			return nil
		},
	}

	mockConn.On("QueryRow", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRow)
	mockConn.On("Query", ctx, mock.AnythingOfType("string"), mock.Anything).Return(nil, errors.New("query failed"))

	result, err := repo.GetMetrics(ctx, query)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get grouped metrics")
}

func TestGetMetrics_InvalidGroupBy(t *testing.T) {
	mockConn := new(MockConn)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	query := &event.GetMetricsQuery{
		EventName: "test_event",
		From:      time.Now().Add(-24 * time.Hour).Unix(),
		To:        time.Now().Unix(),
		GroupBy:   "invalid",
	}
	ctx := context.Background()

	mockRow := &MockRow{
		scanFunc: func(dest ...any) error {
			if len(dest) >= 2 {
				*dest[0].(*uint64) = 100
				*dest[1].(*uint64) = 50
			}
			return nil
		},
	}

	mockConn.On("QueryRow", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRow)

	result, err := repo.GetMetrics(ctx, query)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Nil(t, result.GroupedData)
}

func TestGetMetrics_RowsIterationError(t *testing.T) {
	mockConn := new(MockConn)
	repo := &EventRepository{conn: mockConn, database: "test_db"}

	query := &event.GetMetricsQuery{
		EventName: "test_event",
		From:      time.Now().Add(-24 * time.Hour).Unix(),
		To:        time.Now().Unix(),
		GroupBy:   "channel",
	}
	ctx := context.Background()

	mockRow := &MockRow{
		scanFunc: func(dest ...any) error {
			if len(dest) >= 2 {
				*dest[0].(*uint64) = 100
				*dest[1].(*uint64) = 50
			}
			return nil
		},
	}

	mockRows := NewMockRows([][]any{
		{"web", uint64(60), uint64(30)},
	}, []string{"key", "total_count", "unique_users"})
	mockRows.errVal = errors.New("iteration error")

	mockConn.On("QueryRow", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRow)
	mockConn.On("Query", ctx, mock.AnythingOfType("string"), mock.Anything).Return(mockRows, nil)

	result, err := repo.GetMetrics(ctx, query)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "error iterating rows")
}

func TestNewEventRepository(t *testing.T) {
	mockConn := new(MockConn)
	client := &Client{conn: mockConn, database: "test_db"}

	repo := NewEventRepository(client)

	require.NotNil(t, repo)
	assert.Equal(t, "test_db", repo.database)
}
