package packets

import (
	"container/list"
	"testing"
)

func compareTcpSet(t *testing.T, n int, tcp *tcpSet, want *tcpSet) {
	m := tcp.data.Len()
	if m < want.data.Len() {
		m = want.data.Len()
	}
	eTcp := tcp.data.Front()
	eWant := want.data.Front()
	for i := 0; i < m; i++ {
		if eTcp == nil && eWant == nil {
			t.Fatalf("New().tcp[%d][%d] = nil, test case also nil, all is wrong", n, i)
		} else if eTcp == nil {
			t.Fatalf("New().tcp[%d][%d] = nil, want '%+v'", n, i, *(eWant.Value.(*Event)))
		} else if eWant == nil {
			t.Fatalf("New().tcp[%d][%d] = '%+v', want nil, correct test case", n, i, *(eTcp.Value.(*Event)))
		} else if *(eTcp.Value.(*Event)) != *(eWant.Value.(*Event)) {
			t.Errorf("New().tcp[%d][%d] = '%+v', want '%+v'", n, i, *(eTcp.Value.(*Event)), *(eWant.Value.(*Event)))
		}
		if eTcp != nil {
			eTcp = eTcp.Next()
		}
		if eWant != nil {
			eWant = eWant.Next()
		}
	}
}

func newEvents(es ...*Event) *list.List {
	l := list.New()
	for _, e := range es {
		l.PushBack(e)
	}
	return l
}

func TestNew(t *testing.T) {
	type args struct {
	}
	tests := []struct {
		filename string
		want     *Packets
		wantErr  bool
	}{
		{"test/tcp.txt", &Packets{
			maxTCPConnections: 2,
			tcp: []tcpSet{
				{data: newEvents(
					&Event{CONNECT, 1602515799938828000, ""},
					&Event{SEND, 1602515799938949000, "test.a1 1 1602515799\ntest.a2 1 1602515799\n"},
					&Event{CLOSE, 1602515799938983000, ""},
					&Event{CONNECT, 1602515799959293000, ""},
					&Event{SEND, 1602515799959370000, "test.a3 3 1602515799\n"},
					&Event{SEND, 1602515800957257000, "test.a4 4 1602515800\n"},
					&Event{CLOSE, 1602515800957750000, ""},
					&Event{CONNECT, 1602515801578203000, ""},
					&Event{CLOSE, 1602515801578253000, ""},
				)},
				{data: newEvents(
					&Event{CONNECT, 1602515801578205000, ""},
					&Event{SEND, 1602515801578213000, "test.a5 3 1602515799\n"},
					&Event{CLOSE, 1602515801578223000, ""},
				)},
			},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			p := New()
			err := p.Load(tt.filename)
			if (err != nil) != tt.wantErr {
				t.Fatalf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if p.maxTCPConnections != tt.want.maxTCPConnections {
				t.Errorf("New() maxTCPConnections = %d, want maxTCPConnections %d", p.maxTCPConnections, tt.want.maxTCPConnections)
			}
			if p.maxTCPConnections != len(tt.want.tcp) {
				t.Fatalf("New() maxTCPConnections = %d, test with %d, correct test case", p.maxTCPConnections, len(tt.want.tcp))
			}
			for i := 0; i < p.maxTCPConnections; i++ {
				compareTcpSet(t, i, &p.tcp[i], &tt.want.tcp[i])
			}
		})
	}
}
