package mocks

import (
	"context"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/mock"
)

// MockClickHouseConn is a mock implementation of driver.Conn
type MockClickHouseConn struct {
	mock.Mock
}

func (m *MockClickHouseConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(driver.Batch), args.Error(1)
}

func (m *MockClickHouseConn) QueryRow(ctx context.Context, query string, queryArgs ...any) driver.Row {
	args := m.Called(ctx, query, queryArgs)
	return args.Get(0).(driver.Row)
}

func (m *MockClickHouseConn) Query(ctx context.Context, query string, queryArgs ...any) (driver.Rows, error) {
	args := m.Called(ctx, query, queryArgs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(driver.Rows), args.Error(1)
}

func (m *MockClickHouseConn) Exec(ctx context.Context, query string, queryArgs ...any) error {
	args := m.Called(ctx, query, queryArgs)
	return args.Error(0)
}

func (m *MockClickHouseConn) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockClickHouseConn) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClickHouseConn) Stats() driver.Stats {
	args := m.Called()
	return args.Get(0).(driver.Stats)
}

func (m *MockClickHouseConn) AsyncInsert(ctx context.Context, query string, wait bool, queryArgs ...any) error {
	args := m.Called(ctx, query, wait, queryArgs)
	return args.Error(0)
}

func (m *MockClickHouseConn) Select(ctx context.Context, dest any, query string, queryArgs ...any) error {
	args := m.Called(ctx, dest, query, queryArgs)
	return args.Error(0)
}

func (m *MockClickHouseConn) Contributors() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockClickHouseConn) ServerVersion() (*driver.ServerVersion, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*driver.ServerVersion), args.Error(1)
}

// MockClickHouseBatch is a mock implementation of driver.Batch
type MockClickHouseBatch struct {
	mock.Mock
}

func (m *MockClickHouseBatch) Append(v ...any) error {
	args := m.Called(v)
	return args.Error(0)
}

func (m *MockClickHouseBatch) AppendStruct(v any) error {
	args := m.Called(v)
	return args.Error(0)
}

func (m *MockClickHouseBatch) Column(idx int) driver.BatchColumn {
	args := m.Called(idx)
	return args.Get(0).(driver.BatchColumn)
}

func (m *MockClickHouseBatch) Abort() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClickHouseBatch) Flush() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClickHouseBatch) Send() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClickHouseBatch) IsSent() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockClickHouseBatch) Rows() int {
	args := m.Called()
	return args.Int(0)
}

// MockClickHouseRow is a mock implementation of driver.Row
type MockClickHouseRow struct {
	mock.Mock
	ScanFunc func(dest ...any) error
}

func (m *MockClickHouseRow) Scan(dest ...any) error {
	if m.ScanFunc != nil {
		return m.ScanFunc(dest...)
	}
	args := m.Called(dest)
	return args.Error(0)
}

func (m *MockClickHouseRow) ScanStruct(dest any) error {
	args := m.Called(dest)
	return args.Error(0)
}

func (m *MockClickHouseRow) Err() error {
	args := m.Called()
	return args.Error(0)
}

// MockClickHouseRows is a mock implementation of driver.Rows
type MockClickHouseRows struct {
	mock.Mock
	currentIndex int
	data         [][]any
	columns      []string
	columnTypes  []driver.ColumnType
}

func NewMockClickHouseRows(data [][]any, columns []string) *MockClickHouseRows {
	return &MockClickHouseRows{
		currentIndex: -1,
		data:         data,
		columns:      columns,
	}
}

func (m *MockClickHouseRows) Next() bool {
	m.currentIndex++
	return m.currentIndex < len(m.data)
}

func (m *MockClickHouseRows) Scan(dest ...any) error {
	if m.currentIndex >= len(m.data) {
		return nil
	}
	row := m.data[m.currentIndex]
	for i, v := range row {
		if i < len(dest) {
			reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(v))
		}
	}
	return nil
}

func (m *MockClickHouseRows) ScanStruct(dest any) error {
	args := m.Called(dest)
	return args.Error(0)
}

func (m *MockClickHouseRows) Close() error {
	return nil
}

func (m *MockClickHouseRows) Err() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClickHouseRows) Columns() []string {
	return m.columns
}

func (m *MockClickHouseRows) ColumnTypes() []driver.ColumnType {
	return m.columnTypes
}

func (m *MockClickHouseRows) Totals(dest ...any) error {
	args := m.Called(dest)
	return args.Error(0)
}
