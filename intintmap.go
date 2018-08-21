// Package intintmap is a fast int64 key -> int64 value map.
//
// It is copied nearly verbatim from http://java-performance.info/implementing-world-fastest-java-int-to-int-hash-map/
package intintmap

import (
	"math"
)

const INT_PHI = 0x9E3779B9
const FREE_KEY = 0
const NO_VALUE = 0

func phiMix(x int64) int64 {
	h := x * INT_PHI
	return h ^ (h >> 16)
}

// Map is a map-like data-structure for int64s
type Map struct {
	data       []int64 // interleaved keys and values
	fillFactor float64
	threshold  int // we will resize a map once it reaches this size
	size       int

	mask  int64 // mask to calculate the original position
	mask2 int64

	hasFreeKey bool  // do we have 'free' key in the map?
	freeVal    int64 // value of 'free' key
}

func nextPowerOf2(x uint32) uint32 {
	if x == 0 {
		return 1
	}
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	return (x | x>>32) + 1
}

func arraySize(exp int, fill float64) int {
	s := nextPowerOf2(uint32(math.Ceil(float64(exp) / fill)))
	if s < 2 {
		s = 2
	}
	return int(s)
}

// New returns a map initialized with n spaces and uses the stated fillFactor.
// The map will grow as needed.
func New(size int, fillFactor float64) *Map {
	if fillFactor <= 0 || fillFactor >= 1 {
		panic("FillFactor must be in (0, 1)")
	}
	if size <= 0 {
		panic("Size must be positive")
	}

	capacity := arraySize(size, fillFactor)
	return &Map{
		data:       make([]int64, 2*capacity),
		fillFactor: fillFactor,
		threshold:  int(math.Floor(float64(capacity) * fillFactor)),
		mask:       int64(capacity - 1),
		mask2:      int64(2*capacity - 1),
	}
}

// Get returns the value or NO_VALUE if the key is not found.
func (m *Map) Get(key int64) int64 {
	if key == FREE_KEY {
		if m.hasFreeKey {
			return m.freeVal
		}
		return NO_VALUE
	}

	ptr := (phiMix(key) & m.mask) << 1
	k := m.data[ptr]

	if key == FREE_KEY { // end of chain already
		return NO_VALUE
	}
	if k == key { // we check FREE prior to this call
		return m.data[ptr+1]
	}

	for {
		ptr = (ptr + 2) & m.mask2
		k = m.data[ptr]
		if k == FREE_KEY {
			return NO_VALUE
		}
		if k == key {
			return m.data[ptr+1]
		}
	}
}

// Put adds val to the map under the specified key and returns the old value in that key.
func (m *Map) Put(key int64, val int64) int64 {
	if key == FREE_KEY {
		ret := m.freeVal
		if !m.hasFreeKey {
			m.size += 1
		}
		m.hasFreeKey = true
		m.freeVal = val
		return ret
	}

	ptr := (phiMix(key) & m.mask) << 1
	k := m.data[ptr]

	if k == FREE_KEY { // end of chain already
		m.data[ptr] = key
		m.data[ptr+1] = val
		if m.size >= m.threshold {
			m.rehash()
		} else {
			m.size += 1
		}
		return NO_VALUE
	} else if k == key { // overwrite existed value
		ret := m.data[ptr+1]
		m.data[ptr+1] = val
		return ret
	}

	for {
		ptr = (ptr + 2) & m.mask2
		k = m.data[ptr]

		if k == FREE_KEY {
			m.data[ptr] = key
			m.data[ptr+1] = val
			if m.size >= m.threshold {
				m.rehash()
			} else {
				m.size += 1
			}
			return NO_VALUE
		} else if k == key {
			ret := m.data[ptr+1]
			m.data[ptr+1] = val
			return ret
		}
	}

}

// Del deletes a key and its value, and returns the value or NO_VALUE if the key is not found.
func (m *Map) Del(key int64) int64 {
	if key == FREE_KEY {
		if !m.hasFreeKey {
			return NO_VALUE
		}
		m.hasFreeKey = false
		m.size -= 1
		return m.freeVal
	}

	ptr := (phiMix(key) & m.mask) << 1
	k := m.data[ptr]

	var res int64
	if k == key {
		res = m.data[ptr+1]
		m.shiftKeys(ptr)
		m.size -= 1
		return res
	} else if k == FREE_KEY { // end of chain already
		return NO_VALUE
	}

	for {
		ptr = (ptr + 2) & m.mask2
		k = m.data[ptr]

		if k == key {
			res = m.data[ptr+1]
			m.shiftKeys(ptr)
			m.size -= 1
			return res
		} else if k == FREE_KEY {
			return NO_VALUE
		}

	}
}

func (m *Map) shiftKeys(pos int64) int64 {
	// Shift entries with the same hash.
	var last, slot int64
	var k int64
	var data = m.data
	for {
		last = pos
		pos = (last + 2) & m.mask2
		for {
			k = data[pos]
			if k == FREE_KEY {
				data[last] = FREE_KEY
				return last
			}

			slot = (phiMix(k) & m.mask) << 1
			if last <= pos {
				if last >= slot || slot > pos {
					break
				}
			} else {
				if last >= slot && slot > pos {
					break
				}
			}
			pos = (pos + 2) & m.mask2
		}
		data[last] = k
		data[last+1] = data[pos+1]
	}
}

func (m *Map) rehash() {
	newCapacity := len(m.data) * 2
	m.threshold = int(math.Floor(float64(newCapacity/2) * m.fillFactor))
	m.mask = int64(newCapacity/2 - 1)
	m.mask2 = int64(newCapacity - 1)

	data := make([]int64, len(m.data)) // copy of original data
	copy(data, m.data)

	m.data = make([]int64, newCapacity)
	if m.hasFreeKey { // reset size
		m.size = 1
	} else {
		m.size = 0
	}

	var o int64
	for i := 0; i < len(data); i += 2 {
		o = data[i]
		if o != FREE_KEY {
			m.Put(o, data[i+1])
		}
	}
}

// Size returns size of the map.
func (m *Map) Size() int {
	return m.size
}

// Keys returns a channel for iterating all keys.
func (m *Map) Keys() chan int64 {
	c := make(chan int64, 10)
	go func() {
		data := m.data
		var k int64

		for i := 0; i < len(data); i += 2 {
			k = data[i]
			if k == FREE_KEY {
				if m.hasFreeKey {
					c <- FREE_KEY // value is m.freeVal
				} else {
					continue
				}
			}
			c <- k // value is data[i+1]
		}
		close(c)
	}()
	return c
}

// Items returns a channel for iterating all key-value pairs.
func (m *Map) Items() chan [2]int64 {
	c := make(chan [2]int64, 10)
	go func() {
		data := m.data
		var k int64

		for i := 0; i < len(data); i += 2 {
			k = data[i]
			if k == FREE_KEY {
				if m.hasFreeKey {
					c <- [2]int64{FREE_KEY, m.freeVal}
				} else {
					continue
				}
			}
			c <- [2]int64{k, data[i+1]}
		}
		close(c)
	}()
	return c
}
