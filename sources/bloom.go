package sources

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/minio/highwayhash"
)

const (
	// DefaultAdviseSize is the default expected number of elements.
	DefaultAdviseSize = 75000

	// DefaultErrorRate is the default target error rate.
	// Range 0.0 - 1.0, default value is 1%.
	DefaultErrorRate = 0.01
)

// BloomFilter is a probabilistic data structure that can represent set
// membership, such that one can be fully certain an item is NOT in the set,
// and have a reasonably bounded idea whether an item may be in the set.
// N.B. in order to have confidence in error bounds, the Advise size
// estimate should be greater than the number of elements added.
//
// e.g. the question "is X in the set?" has answers "no" and "maybe"
type BloomFilter struct {
	advised  int64
	estError float64

	nadded uint64
	keys   uint64
	size   uint64

	parts []uint64

	h0State []byte
	h1State []byte
}

func (b *BloomFilter) resize() {
	if b.nadded != 0 {
		panic("cannot resize BloomFilter after elements have been added")
	}
	m, k := estimateBloomParams(uint64(b.advised), b.estError)
	if k < 1 {
		k = 1
	}
	b.size, b.keys = uint64(m), uint64(k)
	b.parts = make([]uint64, 1+(b.size/64))
}

// Advise the Detector on the estimated size of the data set.
func (b *BloomFilter) Advise(size int) {
	b.advised = int64(size)
	if b.estError <= 0.0 {
		b.estError = DefaultErrorRate
	}
	b.resize()
}

// ErrorRate sets the desired error rate for the Detector.
func (b *BloomFilter) ErrorRate(rate float64) {
	b.estError = rate
	if b.advised <= 0 {
		b.advised = DefaultAdviseSize
	}
	b.resize()
}

// Learn a positive value in the data set.
func (b *BloomFilter) Learn(value string) {
	if b.keys == 0 {
		b.resize()
	}
	h0, h1 := b.hash(value)
	hx := h0 % b.size
	for k := uint64(0); k < b.keys; k++ {
		//log.Println(hx / 64)
		b.parts[hx/64] |= 1 << (hx % 64)
		hx = (hx + h1) % b.size
	}
	b.nadded++
}

// Detect predicts the value's inclusion in the data set.
// It returns true/false for the prediction, along with a confidence
// score from 0.0-1.0.  A score of 0.0 means most likely not in the
// set, and a score of 1.0 means most like in the set.
func (b *BloomFilter) Detect(value string) (bool, float64) {
	h0, h1 := b.hash(value)
	hx := h0 % b.size
	for k := uint64(0); k < b.keys; k++ {
		if (b.parts[hx/64] & (1 << (hx % 64))) == 0 {
			return false, 0.0
		}
		hx = (hx + h1) % b.size
	}
	return true, 1.0 - b.estimateError()
}

// Name of the detector instance type.
func (b *BloomFilter) Name() string {
	return "bloom"
}

// Count returns the number of items added to the set (if known)
func (b *BloomFilter) Count() uint64 {
	return b.nadded
}

// Pack the detector into a serializable string.
func (b *BloomFilter) Pack() []byte {
	buf := &bytes.Buffer{}
	gw, _ := gzip.NewWriterLevel(buf, gzip.BestCompression)
	binary.Write(gw, binary.LittleEndian, []uint64{b.size, b.keys, b.nadded})
	binary.Write(gw, binary.LittleEndian, b.parts)
	gw.Close()
	return buf.Bytes()
	//return base64.StdEncoding.EncodeToString(buf.Bytes())
}

// Unpack the detector from a serialized bytes.
func (b *BloomFilter) Unpack(rawbytes []byte) error {
	/*
		rawbytes, err := base64.StdEncoding.DecodeString(packed)
		if err != nil {
			return err
		}*/

	tmp := [3]uint64{0, 0, 0}
	gr, err := gzip.NewReader(bytes.NewReader(rawbytes))
	if err != nil {
		return err
	}
	err = binary.Read(gr, binary.LittleEndian, &tmp)
	if err != nil {
		return err
	}
	b.advised = 0
	b.estError = 0.0
	b.size = tmp[0]
	b.keys = tmp[1]
	b.nadded = tmp[2]

	b.parts = make([]uint64, 1+(b.size/64))

	// force reload on next hash
	b.h0State = b.h0State[:0]
	b.h1State = b.h1State[:0]

	return binary.Read(gr, binary.LittleEndian, b.parts)
}

///////////////////////////////////////////////////////

func (b *BloomFilter) String() string {
	res := fmt.Sprintf("bloom(m=%d, k=%d, n=%d)\n", b.size, b.keys, b.nadded)
	res += fmt.Sprintf("estimated error rate  : %3.3f%%\n", b.estimateError()*100.0)
	res += fmt.Sprintf("optimal keys          : %d\n", b.optimalKeys())

	res += fmt.Sprintf("optimal size (err=1%%) : %d\n", b.optimalSize(0.01))
	m, k := estimateBloomParams(b.nadded, 0.01)
	res += fmt.Sprintf("                params: k=%d, m=%d (%dkb)\n", k, m, (m/8)/1024)
	res += fmt.Sprintf("optimal size (err=10%%): %d\n", b.optimalSize(0.1))
	m, k = estimateBloomParams(b.nadded, 0.1)
	res += fmt.Sprintf("                params: k=%d, m=%d (%dkb)\n", k, m, (m/8)/1024)
	res += fmt.Sprintf("optimal size (err=30%%): %d\n", b.optimalSize(0.3))
	m, k = estimateBloomParams(b.nadded, 0.3)
	res += fmt.Sprintf("                params: k=%d, m=%d (%dkb)\n", k, m, (m/8)/1024)
	return res
}

/// mf = n*log(errRate)/(-ln 2)^2
func estimateBloomParams(n uint64, errRate float64) (m, k int) {
	div := 1.0 / (-math.Ln2 * math.Ln2)
	mf := float64(n) * math.Log(errRate) * div
	kf := math.Ln2 * mf / float64(n)
	return int(mf), int(kf)
}

func (b *BloomFilter) optimalSize(errRate float64) int {
	div := 1.0 / (-math.Ln2 * math.Ln2)
	return int(float64(b.nadded) * math.Log(errRate) * div)
}

func (b *BloomFilter) optimalKeys() int {
	return int(math.Ln2 * float64(b.size) / float64(b.nadded))
}

// b=bits per element
// (1.0 - e^(-k/b))^k
func (b *BloomFilter) estimateError() float64 {
	return math.Pow(1.0-math.Exp(-float64(b.keys*b.nadded)/float64(b.size)), float64(b.keys))
}

//////////////

func (b *BloomFilter) hash(value string) (uint64, uint64) {
	if len(b.h0State) == 0 {
		b.h0State = make([]byte, 32)
		b.h1State = make([]byte, 32)
		// we care about reproducability, not uniqueness...
		binary.LittleEndian.PutUint64(b.h0State, b.size)
		binary.LittleEndian.PutUint64(b.h1State, b.keys)
	}

	h0 := highwayhash.Sum64([]byte(value), b.h0State)
	h1 := highwayhash.Sum64([]byte(value), b.h1State)
	return h0, h1
}
