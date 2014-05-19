package main

import (
	"github.com/boltdb/bolt"
	"log"
	"encoding/binary"
	"bytes"
	"math"
)

var EVENTS_BUCKET = []byte("events")
var POINTERS_BUCKET = []byte("pointers")
var READ_POINTER = []byte("read-pointer")

type Store struct {
	db              *bolt.DB
	eventsIn        chan *EventIn
	eventsOut       chan *EventOut
	eventsReRead    chan *EventOut
	eventsDelivered chan int64
	eventsFailed    chan int64
	readPointer     int64
	writePointer    int64
	readTrigger     chan bool
	stopStore       chan bool
	stopRead        chan bool
	stopClean       chan bool
	shutdown        chan bool
}

func (s *Store) EventsSequence() int64 {
	return s.writePointer
}

func (s *Store) EventsInChannel() chan <- *EventIn {
	return s.eventsIn
}

func (s *Store) EventsOutChannel() <- chan *EventOut {
	return s.eventsOut
}

func OpenStore(dbFile string) (*Store, error) {
	log.Printf("go=open at=open-db\n")
	db, err := bolt.Open(dbFile, 0600)
	//db.StrictMode = true
	if err != nil {
		log.Printf("go=open at=error-openiing-db error=%s\n", err)
		return nil, err
	}
	lastWritePointer, readPointer, err := findReadAndWritePointers(db)
	if err != nil {
		log.Printf("go=open at=read-pointers-error error=%s\n", err)
		return nil, err
	}
	log.Printf("go=open at=read-pointers read=%d write=%d\n", readPointer, lastWritePointer)

	store := &Store{
		db:db,
		eventsIn: make(chan *EventIn),
		eventsOut: make(chan *EventOut, 32),
		eventsReRead: make(chan *EventOut, 32),
		eventsDelivered: make(chan int64, 32),
		eventsFailed: make(chan int64, 32),
		writePointer: lastWritePointer + 1,
		readPointer: readPointer,
		readTrigger:  make(chan bool, 1), //buffered so reader can send to itself
		stopStore:  make(chan bool, 1),
		stopRead:  make(chan bool, 1),
		stopClean:  make(chan bool, 1),
		shutdown: make(chan bool, 3),
	}

	go store.readEvents()
	go store.cleanEvents()
	go store.storeEvents()
	log.Printf("go=open at=store-created")
	return store, nil
}

func (s *Store) Close() error {
	s.stopStore <- true
	s.stopClean <- true
	s.stopRead <- true
	//drain events out so the readEvents goroutine can unblock and exit.
	s.drainEventsOut()
	//close eventsDelivered, eventsFailed so cleanEvents can unblock and exit.
	//todo how to do this when senders are happeining
	close(s.eventsIn)
	close(s.eventsDelivered)
	close(s.eventsFailed)
	close(s.eventsReRead)
	<-s.shutdown
	<-s.shutdown
	<-s.shutdown
	return s.db.Close()
}

// store events runs in a goroutine and receives from s.eventsIn and saves events to bolt
func (s *Store) storeEvents() {
	for {
		select {
		case e, ok := <-s.eventsIn:
			if ok {
				err := s.writeEvent(s.writePointer, e)
				if err == nil {
					s.writePointer += 1
					s.triggerRead()
				} else {
					log.Printf("go=store at=write-error error=%s", err)
				}
				e.saved <- err == nil
			}
		case <-s.stopStore:
			log.Println("go=store at=shtudown-store")
		    s.shutdown <- true
			return
		}

	}
	log.Println("go=store at=exit")

}

// readEvents runs in a goroutine and reads events from boltdb and sends them on s.eventsOut
// it also receives from the events reRead channel and resends on s.eventsOut
// it is the owning sender for s.eventsOut, so it closes the channel on shutdown.
func (s *Store) readEvents() {
	for {
		select {
		case <-s.stopRead:
			log.Println("go=read at=shutdown-read")
			close(s.eventsOut)
			//unblock the cleanEvents send if necessary
			go s.drainEventsReRead()
		    s.shutdown <- true
			return
		case e, ok := <-s.eventsReRead:
			if ok {
				s.eventsOut <- e
			}
		case _, ok := <-s.readTrigger:
			if ok {
				log.Printf("go=read at=begin sequence=%d", s.readPointer)
				event, err := s.readEvent(s.readPointer)
				if event != nil {
					s.eventsOut <- event
					s.readPointer += 1
					s.triggerRead()
				}
				if err != nil {
					log.Printf("go=read at=read-error error=%s", err)
				}
			}
		}

	}
	log.Println("go=read at=exit")
}

// cleanEvents runs in a goroutine and  recieves from s.eventsDelivered and s.eventsFailed.
// for delivered events it removes the event from bolt
// for failed events it reReads the event and sends it on
// owning sender for s.eventsReRead so it closes it on shutdown
func (s *Store) cleanEvents() {
	for {
		select {
		case <-s.stopClean:
			log.Println("go=clean at=shutdown-clean")
		    s.shutdown <- true
			return
		case delivered, ok := <-s.eventsDelivered:
			if ok {
				s.deleteEvent(delivered)
			}
		case failed, ok := <-s.eventsFailed:
			if ok {
				event, err := s.readEvent(failed)
				if err != nil {
					log.Printf("go=clean at=re-read-error error=%s", err)
				} else {
					s.eventsReRead <- event
				}
			}
		}
	}

	log.Println("go=clean at=exit")
}

func (s *Store) writeEvent(seq int64, e *EventIn) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(EVENTS_BUCKET)
		err := bucket.Put(writeSequence(seq), e.body)
		if err != nil {
			log.Printf("go=store at=put-fail error=%s\n", err)
			return err
		}

		if seq%1000 == 0 {
			log.Printf("go=store at=wrote sequence=%d", seq)
		}
		return nil

	})
	return err
}

// reRead event reads an event that was reported to have failed to be delivered and reSends it on s.eventsReRead
func (s *Store) readEvent(seq int64) (*EventOut, error) {
	var eventOut *EventOut
	err := s.db.Update(func(tx *bolt.Tx) error {
		events := tx.Bucket(EVENTS_BUCKET)
		event := events.Get(writeSequence(seq))
		if event == nil {
			return nil
		} else {
			if seq%1000 == 0 {
				log.Printf("go=read at=read sequence=%d", seq)
			}
			eventOut = &EventOut{sequence:seq, body:event}
			return nil
		}
	})
	return eventOut, err
}

func (s *Store) deleteEvent(seq int64) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		events := tx.Bucket(EVENTS_BUCKET)
		err := events.Delete(writeSequence(seq))
		return err

	})
	if err != nil {
		log.Printf("go=delete at=delete-error error=%s", err)
	}
	return err
}

func (s *Store) triggerRead() {
	select {
	case s.readTrigger <- true:
	default:
	}
}

func (s *Store) drainEventsOut() {
	for {
		_, ok := <-s.eventsOut
		if !ok {
			return
		}
	}
}

func (s *Store) drainEventsReRead() {
	for {
		select {
		case _, ok := <-s.eventsReRead:
			if !ok {
				return
			}
		default:
		}
	}
}

func getReadPointer(tx *bolt.Tx) (int64, error) {
	pointers := tx.Bucket(POINTERS_BUCKET)
	readBytes := pointers.Get(READ_POINTER)
	if readBytes == nil {
		return 1, nil
	} else {
		return readSequence(readBytes)
	}
}

func findReadAndWritePointers(db *bolt.DB) (int64, int64, error) {
	writePointer := int64(0)
	readPointer := int64(math.MaxInt64)
	err := db.Update(func(tx *bolt.Tx) error {
		events, err := tx.CreateBucketIfNotExists(EVENTS_BUCKET)
		if err != nil {
			return err
		}

		err = events.ForEach(func(k, v []byte) error {
			seq, _ := readSequence(k)
			if seq > writePointer {
				writePointer = seq
			}
			if seq < readPointer {
				readPointer = seq
			}
			return nil
		})

		return err
	})

	if err != nil {
		return -1, -1, err
	}

	if readPointer == int64(math.MaxInt64) {
		readPointer = int64(1)
	}

	return writePointer, readPointer, nil
}

func readSequence(seq []byte) (int64, error) {
	return binary.ReadVarint(bytes.NewBuffer(seq))
}

func writeSequence(seq int64) []byte {
	buffer := make([]byte, 16)
	binary.PutVarint(buffer, seq)
	return buffer
}
