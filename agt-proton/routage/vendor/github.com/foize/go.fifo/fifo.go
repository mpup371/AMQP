// Created by Yaz Saito on 06/15/12.
// Modified by Geert-Johan Riemer, Foize B.V.

package fifo

const initChunkSize uint = 64

// chunks are used to make a queue auto resizeable.
type chunk struct {
	items       []interface{} // list of queue'ed items
	size        uint          // dynamic size allocation
	first, last uint          // positions for the first and last item in this chunk
	next        *chunk        // pointer to the next chunk (if any)
}

// newChunk allocate a chunk of demanded size
//TODO error if not enough memory
func newChunk(size uint) *chunk {
	return &chunk{
		make([]interface{}, size),
		size,
		0, 0,
		nil,
	}
}

// Queue is a fifo queue
type Queue struct {
	head, tail   *chunk // chunk head and tail
	count        uint   // total amount of items in the queue
	curChunkSize uint   // current chunk size for new allocation
}

// NewQueue creates a new and empty *fifo.Queue
func NewQueue() (q Queue) {
	initChunk := newChunk(initChunkSize)
	return Queue{
		head:         initChunk,
		tail:         initChunk,
		curChunkSize: initChunkSize,
	}
}

// addChunk will allocate new chunks depending on actuel queue size
func (q *Queue) addChunk() {
	if q.count >= 2*q.curChunkSize {
		q.curChunkSize = q.count
	}
	q.tail.next = newChunk(q.curChunkSize)
	q.tail = q.tail.next
}

// Len Return the number of items in the queue
func (q *Queue) Len() (length uint) {
	return q.count
}

// Add an item to the end of the queue
func (q *Queue) Add(item interface{}) uint {

	// check if item is valid
	if item == nil {
		panic("can not add nil item to fifo queue")
	}

	// if the tail chunk is full, create a new one and add it to the queue.
	if q.tail.last >= q.tail.size {
		q.addChunk()
	}

	// add item to the tail chunk at the last position
	q.tail.items[q.tail.last] = item
	q.tail.last++
	q.count++
	return q.count
}

// Next remove the item at the head of the queue and return it.
// Returns nil when there are no items left in queue.
func (q *Queue) Next() (item interface{}, length uint) {

	// Return nil if there are no items to return
	if q.count == 0 {
		return nil, 0
	}

	// Get item from queue
	item = q.head.items[q.head.first]

	//allow GC item later
	q.head.items[q.head.first] = nil

	// increment first position and decrement queue item count
	q.head.first++
	q.count--

	if q.head.first >= q.head.last {
		// we're at the end of this chunk and we should do some maintainance
		// if there are no follow up chunks then reset the current one so it can be used again.
		if q.count == 0 {
			q.curChunkSize = initChunkSize
			q.head = newChunk(q.curChunkSize) // free memory in case of big empty chunk
			q.head.first = 0
			q.head.last = 0
			q.head.next = nil
			q.tail = q.head
		} else {
			// set queue's head chunk to the next chunk
			// old head will fall out of scope and be GC-ed
			q.head = q.head.next
		}
	}

	// return the retrieved item
	return item, q.count
}

// Peek returns the first item in the queue without removing it
// Returns nil when there are no items left in queue.
func (q *Queue) Peek() (item interface{}) {

	// Return nil if there are no items to return
	if q.count == 0 {
		return nil
	}
	// FIXME: why would this check be required?
	// if q.head.first >= q.head.last {
	// 	return nil
	// }

	// Get item from queue
	item = q.head.items[q.head.first]

	if q.head.first >= q.head.last {
		// we're at the end of this chunk and we should do some maintainance
		// if there are no follow up chunks then reset the current one so it can be used again.
		if q.count == 0 {
			q.head.first = 0
			q.head.last = 0
			q.head.next = nil
		} else {
			// set queue's head chunk to the next chunk
			// old head will fall out of scope and be GC-ed
			q.head = q.head.next
		}
	}

	// return the retrieved item
	return item
}

// Pop remove the item at the head of the queue wihtout return it.
// Returns nil when there are no items left in queue.
func (q *Queue) Pop() uint {

	// Return nil if there are no items to return
	if q.count == 0 {
		return 0
	}

	//allow GC item later
	q.head.items[q.head.first] = nil

	// increment first position and decrement queue item count
	q.head.first++
	q.count--

	if q.head.first >= q.head.last {
		// we're at the end of this chunk and we should do some maintainance
		// if there are no follow up chunks then reset the current one so it can be used again.
		if q.count == 0 {
			q.head.first = 0
			q.head.last = 0
			q.head.next = nil
		} else {
			// set queue's head chunk to the next chunk
			// old head will fall out of scope and be GC-ed
			q.head = q.head.next
		}
	}
	return q.count
}
