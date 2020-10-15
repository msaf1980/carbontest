package packets

import (
	"bufio"
	"container/list"
	"io"
	"os"
	"strconv"
	"strings"

	"carbontest/pkg/base"
)

type iEvent struct {
	key       string
	proto     base.Proto
	timestamp int64
	action    base.NetOper
}

type tcpSet struct {
	data   *list.List
	last   int64        // last action timestamp
	status base.NetOper // last action
}

type Packets struct {
	start             int64
	maxTCPConnections int // maximum opended connection
	tcpConn           map[string]int
	tcp               []tcpSet
}

// New allocate packets file
func New() *Packets {
	p := new(Packets)
	p.tcpConn = make(map[string]int)
	p.tcp = make([]tcpSet, 32768)

	return p
}

// Load load packets file
func (p *Packets) Load(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReader(file)

	var e iEvent
	var sb strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
		if line[0] == '#' {
			p.flush(&e, &sb)
			fields := strings.Split(line, " ")
			t := strings.Split(fields[0][1:], ".")
			sec, err := strconv.ParseInt(t[0], 10, 64)
			if err != nil {
				continue
			}
			usec, err := strconv.ParseInt(t[1], 10, 64)
			if err != nil {
				continue
			}
			e.timestamp = sec*1000000000 + usec*1000
			if p.start == 0 || p.start > e.timestamp {
				p.start = e.timestamp
			}
			if fields[1] == "TCP" {
				e.proto = base.TCP
				// key is src_ip:sport->dst_ip:dport
				switch fields[5] {
				case "CONNECT\n":
					e.key = fields[2] + fields[3] + fields[4]
					e.action = base.CONNECT
				case "SEND\n":
					e.key = fields[2] + fields[3] + fields[4]
					e.action = base.SEND
				case "FIN\n":
					e.key = fields[2] + fields[3] + fields[4]
					e.action = base.CLOSE
				case "RST\n":
					e.key = fields[4] + fields[3] + fields[2]
					e.action = base.CLOSE
				}
			}
		} else if e.action == base.SEND {
			sb.WriteString(line)
		}
	}
	p.close(&e, &sb)
	return nil
}

func (p *Packets) flush(e *iEvent, sb *strings.Builder) {
	var s string
	if e.action == base.INIT {
		return
	} else if e.proto == base.TCP {
		v, ok := p.tcpConn[e.key]
		if !ok {
			var i int
			for i = 0; i < len(p.tcp); i++ {
				if p.tcp[i].status == base.INIT {
					p.tcp[i].data = list.New()
					if i > p.maxTCPConnections {
						p.maxTCPConnections = i + 1
					}
					break
				} else if p.tcp[i].status == base.CLOSE {
					break
				}
			}
			v = i
			if v == len(p.tcp) {
				t := make([]tcpSet, 2*len(p.tcp))
				copy(t, p.tcp)
				p.tcp = t
			}
			p.tcpConn[e.key] = v
		} else if e.action == base.CLOSE {
			delete(p.tcpConn, e.key)
		}
		if e.action == base.SEND {
			s = sb.String()
			s = s[0 : len(s)-1]
		}
		p.tcp[v].data.PushBack(&base.Event{
			Action: e.action,
			Delay:  e.timestamp,
			Send:   s,
		})
		p.tcp[v].last = e.timestamp
		p.tcp[v].status = e.action
	}
	e.action = base.INIT
	sb.Reset()
}

func (p *Packets) close(e *iEvent, sb *strings.Builder) {
	p.flush(e, sb)
	p.tcpConn = nil
}
