package prometheus

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"wayseer.dev/sdk"
)

// oldPoints is the decoder samples replaced, kept as the reference for its results.
func oldPoints(values [][2]json.RawMessage, w sdk.TimeWindow) ([]sdk.Point, error) {
	out := make([]sdk.Point, 0, len(values))
	for _, v := range values {
		t, err := strconv.ParseFloat(string(v[0]), 64)
		if err != nil {
			return nil, fmt.Errorf("sample time %s: %w", v[0], err)
		}
		var s string
		if err := json.Unmarshal(v[1], &s); err != nil {
			return nil, fmt.Errorf("sample value %s: %w", v[1], err)
		}
		f, err := strconv.ParseFloat(s, 64)
		ns := int64(math.Round(t*1e3)) * int64(time.Millisecond)
		if err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) && w.Contains(ns) {
			out = append(out, sdk.Point{T: ns, V: f})
		}
	}
	return out, nil
}

// sameAsOld checks values decode to the points, or the error, the old decoder gave.
func sameAsOld(t *testing.T, values string) {
	t.Helper()
	w := sdk.TimeWindow{From: time.Unix(100, 0), To: time.Unix(110, 0)}
	var old struct{ Values [][2]json.RawMessage }
	var cur struct{ Values samples }
	oldErr := json.Unmarshal([]byte(`{"Values":`+values+`}`), &old)
	curErr := json.Unmarshal([]byte(`{"Values":`+values+`}`), &cur)
	if (oldErr == nil) != (curErr == nil) {
		t.Fatalf("%s: decoding gave %v, the old decoder %v", values, curErr, oldErr)
	}
	if oldErr != nil {
		return
	}
	want, wantErr := oldPoints(old.Values, w)
	got, err := points(cur.Values, w)
	if fmt.Sprint(err) != fmt.Sprint(wantErr) || !slices.Equal(got, want) {
		t.Errorf("%s: points %v, %v; the old decoder %v, %v", values, got, err, want, wantErr)
	}
}

var sampleCases = []string{
	`[]`,
	`null`,
	`[[99.5,"1"],[100,"2"],[100.25,"NaN"],[101,"+Inf"],[102,"-Inf"],[103,"3"],[110,"4"],[110.001,"5"]]`,
	` [ [ 100.5 , "2.5" ] , [101,"1e400"] ,[102,"x"],[103,""] ] `,
	`[[100,"1"],[1e400,"2"]]`,
	`[[100,"1"],["101","2"]]`,
	`[[100,"1"],[null,"2"]]`,
	`[[100,1]]`,
	`[[100,null]]`,
	`[[100,"1"]]`,
	`[[100,"1\"2"]]`,
	`[[100]]`,
	`[[100,"1","extra"]]`,
	`[[100,"1"],{}]`,
	`[[[100],"1"]]`,
	`[[1.0000000000000002e2,"0x1p3"],[104.9994,"-0"],[105.0005,"1_0"]]`,
	`{"a":1}`,
	`"text"`,
}

func TestSamplesDecodeAsTheOldDecoderDid(t *testing.T) {
	for _, c := range sampleCases {
		sameAsOld(t, c)
	}
}

func FuzzSamples(f *testing.F) {
	for _, c := range sampleCases {
		f.Add(c)
	}
	f.Fuzz(func(t *testing.T, values string) {
		if !json.Valid([]byte(values)) {
			return
		}
		sameAsOld(t, values)
	})
}

func TestSamplesDecodeWithoutAnAllocationEach(t *testing.T) {
	body := []byte(`{"Values":` + string(generatedValues(100, 1000)) + `}`)
	var v struct{ Values samples }
	allocs := testing.AllocsPerRun(20, func() {
		if err := json.Unmarshal(body, &v); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 5 {
		t.Errorf("1000 samples took %.0f allocations to decode", allocs)
	}
}

// generatedValues are n samples from start a second apart, with values as Prometheus writes them.
func generatedValues(start float64, n int) []byte {
	b := []byte{'['}
	for i := range n {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '[')
		b = strconv.AppendFloat(b, start+float64(i), 'f', -1, 64)
		b = append(b, `,"`...)
		b = strconv.AppendFloat(b, 100*math.Abs(math.Sin(float64(i)/7)), 'f', -1, 64)
		b = append(b, `"]`...)
	}
	return append(b, ']')
}
