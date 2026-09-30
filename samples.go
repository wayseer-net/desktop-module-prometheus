package prometheus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

// samples are a range result's [time, "value"] pairs, read without an allocation per sample.
type samples struct {
	at  []sample
	err error // the first sample whose time or value is not the shape Prometheus writes
}

// sample is one pair; ok is false for a value that is not a finite number.
type sample struct {
	t, v float64
	ok   bool
}

// UnmarshalJSON reads the pairs directly, or through encoding/json when they are not in the
// plain shape Prometheus writes.
func (s *samples) UnmarshalJSON(b []byte) error {
	if plain, ok := plainSamples(b); ok {
		*s = plain
		return nil
	}
	var raw [][2]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	*s = rawSamples(raw)
	return nil
}

// rawSamples reads pairs as encoding/json splits them, stopping at the first that is unreadable.
func rawSamples(raw [][2]json.RawMessage) samples {
	out := samples{at: make([]sample, 0, len(raw))}
	for _, v := range raw {
		t, err := strconv.ParseFloat(string(v[0]), 64)
		if err != nil {
			out.err = fmt.Errorf("sample time %s: %w", v[0], err)
			return out
		}
		var s string
		if err := json.Unmarshal(v[1], &s); err != nil {
			out.err = fmt.Errorf("sample value %s: %w", v[1], err)
			return out
		}
		out.at = append(out.at, value(t, s))
	}
	return out
}

func value[T string | []byte](t float64, s T) sample {
	f, err := strconv.ParseFloat(string(s), 64)
	return sample{t: t, v: f, ok: err == nil && !math.IsNaN(f) && !math.IsInf(f, 0)}
}

// plainSamples reads b if it is an array of [number, "string without escapes"]; ok is false
// for any other shape. b is valid JSON, as encoding/json checks it first.
func plainSamples(b []byte) (samples, bool) {
	p := scan{b: b}
	if !p.eat('[') {
		return samples{}, false
	}
	out := samples{at: make([]sample, 0, bytes.Count(b, []byte("],"))+1)}
	if p.eat(']') {
		return out, p.done()
	}
	for {
		var tb, vb []byte
		if !p.eat('[') || !p.number(&tb) || !p.eat(',') || !p.str(&vb) || !p.eat(']') {
			return samples{}, false
		}
		t, err := strconv.ParseFloat(string(tb), 64)
		if err != nil && out.err == nil {
			out.err = fmt.Errorf("sample time %s: %w", tb, err)
		}
		if out.err == nil {
			out.at = append(out.at, value(t, vb))
		}
		switch {
		case p.eat(','):
		case p.eat(']'):
			return out, p.done()
		default:
			return samples{}, false
		}
	}
}

// scan walks JSON bytes, skipping whitespace between tokens.
type scan struct {
	b []byte
	i int
}

func (p *scan) skip() {
	for p.i < len(p.b) && (p.b[p.i] == ' ' || p.b[p.i] == '\t' || p.b[p.i] == '\n' || p.b[p.i] == '\r') {
		p.i++
	}
}

func (p *scan) eat(c byte) bool {
	p.skip()
	if p.i < len(p.b) && p.b[p.i] == c {
		p.i++
		return true
	}
	return false
}

func (p *scan) done() bool {
	p.skip()
	return p.i == len(p.b)
}

// number takes a JSON number's bytes.
func (p *scan) number(out *[]byte) bool {
	p.skip()
	start := p.i
	for p.i < len(p.b) && bytes.IndexByte([]byte("+-.0123456789eE"), p.b[p.i]) >= 0 {
		p.i++
	}
	*out = p.b[start:p.i]
	return p.i > start
}

// str takes a string's bytes between its quotes, if it has no escapes.
func (p *scan) str(out *[]byte) bool {
	if !p.eat('"') {
		return false
	}
	end := bytes.IndexAny(p.b[p.i:], `"\`)
	if end < 0 || p.b[p.i+end] != '"' {
		return false
	}
	*out = p.b[p.i : p.i+end]
	p.i += end + 1
	return true
}
