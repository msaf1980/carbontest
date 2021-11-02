package main

import (
	"carbontest/pkg/base"
)

type Stat struct {
	size uint64

	// not for datagram, only for stream
	// connsElapsed   float64 // total time for connect durration (ms)
	conns          uint64 // total succesed
	connsErr       uint64 // total errors
	connsFileLimit uint64
	connsTout      uint64
	connsReset     uint64
	connsRefused   uint64
	connsResolve   uint64

	// for datagram and stream
	// sendsElapsed   float64 // total time for send durration (ms)
	sends          uint64 // total sended metrics
	sendsErr       uint64 // total errors
	sendsTout      uint64
	sendsReset     uint64
	sendsEOF       uint64 // write error
	sendsFileLimit uint64
	sendsResolve   uint64
}

func (s *Stat) Add(c *ConStat) {
	switch c.Type {
	case base.CONNECT:
		// s.connsElapsed += float64(c.Elapsed) / 1000000.0
		switch c.Error {
		case base.OK:
			s.conns++
		case base.REFUSED:
			s.connsRefused++
			s.connsErr++
		case base.RESET:
			s.connsReset++
			s.connsErr++
		case base.TIMEOUT:
			s.connsTout++
			s.connsErr++
		case base.FILELIMIT:
			s.connsFileLimit++
			s.connsErr++
		case base.LOOKUP:
			s.connsResolve++
			s.connsErr++
		default: // other errors
			s.connsErr++
		}
	case base.SEND:
		// s.sendsElapsed += float64(c.Elapsed) / 1000000.0
		s.size += uint64(c.Size)
		switch c.Error {
		case base.OK:
			s.sends++
		case base.TIMEOUT:
			s.sendsTout++
			s.sendsErr++
		case base.RESET:
			s.sendsReset++
			s.sendsErr++
		case base.EOF:
			s.sendsEOF++
			s.sendsErr++
		case base.FILELIMIT:
			s.sendsFileLimit++
			s.sendsErr++
		case base.LOOKUP:
			s.sendsResolve++
			s.sendsErr++
		default: // other errors
			s.sendsErr++
		}
	}
}

func (s *Stat) Clear() {
	s.size = 0

	s.conns = 0
	s.connsErr = 0
	s.connsFileLimit = 0
	s.connsTout = 0
	s.connsReset = 0
	s.connsRefused = 0
	s.connsResolve = 0

	s.sends = 0
	s.sendsErr = 0
	s.sendsFileLimit = 0
	s.sendsTout = 0
	s.sendsResolve = 0
}
