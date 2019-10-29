package gdpr

import (
	"bytes"
	"github.com/anchorfree/data-go/pkg/geo"
	"github.com/anchorfree/data-go/pkg/types"
	"net"
)

type EventIterator struct {
	iterator types.EventIterator
	event    *types.Event
	err      error

	geoSet *geo.Geo
}

var _ types.EventIterator = (*EventIterator)(nil)

func NewIterator(eventIterator types.EventIterator, geoSet *geo.Geo) *EventIterator {
	return &EventIterator{
		iterator: eventIterator,
		geoSet:   geoSet,
	}
}

func (ei *EventIterator) Next() bool {
	if !ei.iterator.Next() {
		ei.err = ei.iterator.Err()
		return false
	}

	ei.event = ei.iterator.At()
	ei.event.Message = ei.ApplyGDPR(ei.event.Message)

	return true
}

func (ei *EventIterator) At() *types.Event {
	return ei.event
}

func (ei *EventIterator) Err() error {
	return ei.err
}

func (ei *EventIterator) ApplyGDPR(message []byte) []byte {
	for _, ip := range findIPs(message) {
		if net.ParseIP(string(ip)) != nil {
			if ei.geoSet.Get(string(ip)) != "af" {
				message = bytes.Replace(message, ip, []byte(ipGDPR(string(ip))), -1)
			}
		}
	}
	return message
}