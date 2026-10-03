package prometheus

import (
	"errors"
	"fmt"

	"wayseer.dev/sdk"
)

// errLeftOut is a flow end that would make an entity past the bound.
var errLeftOut = errors.New("past max_made")

// maker makes entities for flow ends with make that no module found, joining one refresh's world.
type maker struct {
	w      *world
	max    int
	made   map[sdk.EntityRef]bool
	left   map[sdk.EntityRef]bool // ends left out past max
	skipFn func(sdk.EntityRef) bool
	known  map[endValue]resolved // what each end's value resolved to this refresh
}

type endValue struct {
	end   flowEnd
	value string
}

type resolved struct {
	ref sdk.EntityRef
	err error
}

func newMaker(w *world, bound int) *maker {
	mk := &maker{w: w, max: bound, made: map[sdk.EntityRef]bool{}, left: map[sdk.EntityRef]bool{}, known: map[endValue]resolved{}}
	mk.skipFn = mk.skip
	return mk
}

// resolve is the entity value names: one found in the world, else, with make, one made for it.
// A value names the same entity all refresh, as the world it is matched in stays the same.
func (mk *maker) resolve(r sdk.Resolver, e flowEnd, value string) (sdk.EntityRef, error) {
	k := endValue{e, value}
	if got, ok := mk.known[k]; ok {
		return got.ref, got.err
	}
	ref, err := mk.find(r, e, value)
	mk.known[k] = resolved{ref, err}
	return ref, err
}

// find leaves the module's own made entities out of the match, so a found one takes their place.
func (mk *maker) find(r sdk.Resolver, e flowEnd, value string) (sdk.EntityRef, error) {
	if !e.Make {
		return r.Match(e.Kind, value)
	}
	ref, err := r.MatchExcept(e.Kind, value, mk.skipFn)
	if !errors.Is(err, sdk.ErrNoMatch) {
		return ref, err
	}
	return mk.make(e, value)
}

// skip is true of the module's entities that are not this refresh's targets: those it made.
func (mk *maker) skip(ref sdk.EntityRef) bool {
	if ref.Module() != mk.w.src {
		return false
	}
	_, ours := mk.w.ents[ref]
	return !ours || mk.made[ref]
}

// make adds an entity of e's kind named value, or uses the module's own of that ref.
func (mk *maker) make(e flowEnd, value string) (sdk.EntityRef, error) {
	ref, err := sdk.NewEntityRef(string(mk.w.src), e.Kind, value)
	if err != nil {
		return "", sdk.ErrNoMatch
	}
	if _, ok := mk.w.ents[ref]; ok {
		return ref, nil
	}
	if len(mk.made) >= mk.max {
		mk.left[ref] = true
		return "", errLeftOut
	}
	mk.add(sdk.Entity{Ref: ref, Kind: e.Kind, Name: value, Source: mk.w.src, Attrs: map[string]sdk.Value{"flow_label": sdk.String(e.Label)}})
	return ref, nil
}

func (mk *maker) add(e sdk.Entity) {
	mk.w.ents[e.Ref], mk.made[e.Ref] = e, true
}

// keep adds again the entities a failed query's kept edges join.
func (mk *maker) keep(es []sdk.Entity) {
	for _, e := range es {
		if _, ok := mk.w.ents[e.Ref]; !ok {
			mk.add(e)
		}
	}
}

// touched are the made entities each flow's edges join.
func (mk *maker) touched(flows [][]sdk.Edge) [][]sdk.Entity {
	out := make([][]sdk.Entity, len(flows))
	for i, es := range flows {
		seen := map[sdk.EntityRef]bool{}
		for _, e := range es {
			for _, ref := range []sdk.EntityRef{e.From, e.To} {
				if mk.made[ref] && !seen[ref] {
					seen[ref] = true
					out[i] = append(out[i], mk.w.ents[ref])
				}
			}
		}
	}
	return out
}

func (mk *maker) note() string {
	if len(mk.left) == 0 {
		return ""
	}
	return fmt.Sprintf("flows: %d ends left out past max_made %d", len(mk.left), mk.max)
}
