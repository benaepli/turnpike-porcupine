package engine

import (
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

// scanner reads the payload shapes the simulator writes: an array of value
// objects, each with exactly the members "type" and "value". It gives up
// (ok false) on anything else, and the caller then takes the general
// decoder, so the scanner never decides a row the decoder would read
// differently.
type scanner struct {
	s   string
	i   int
	bad bool
}

func (p *scanner) ws() {
	for p.i < len(p.s) {
		switch p.s[p.i] {
		case ' ', '\t', '\n', '\r':
			p.i++
		default:
			return
		}
	}
}

func (p *scanner) eat(c byte) bool {
	p.ws()
	if p.i < len(p.s) && p.s[p.i] == c {
		p.i++
		return true
	}
	return false
}

func (p *scanner) peek() byte {
	p.ws()
	if p.i < len(p.s) {
		return p.s[p.i]
	}
	return 0
}

// str reads a JSON string. It decodes escapes and rejects control
// characters and invalid UTF-8, which the general decoder would replace.
func (p *scanner) str() string {
	if !p.eat('"') {
		p.bad = true
		return ""
	}
	start := p.i
	for p.i < len(p.s) {
		c := p.s[p.i]
		if c == '"' {
			out := p.s[start:p.i]
			p.i++
			return out
		}
		if c == '\\' {
			return p.strSlow(start)
		}
		if c < 0x20 || c >= utf8.RuneSelf {
			return p.strSlow(start)
		}
		p.i++
	}
	p.bad = true
	return ""
}

func (p *scanner) strSlow(start int) string {
	b := []byte(p.s[start:p.i])
	for p.i < len(p.s) {
		c := p.s[p.i]
		switch {
		case c == '"':
			p.i++
			return string(b)
		case c < 0x20:
			p.bad = true
			return ""
		case c == '\\':
			if p.i+1 >= len(p.s) {
				p.bad = true
				return ""
			}
			e := p.s[p.i+1]
			p.i += 2
			switch e {
			case '"', '\\', '/':
				b = append(b, e)
			case 'b':
				b = append(b, '\b')
			case 'f':
				b = append(b, '\f')
			case 'n':
				b = append(b, '\n')
			case 'r':
				b = append(b, '\r')
			case 't':
				b = append(b, '\t')
			case 'u':
				r, ok := p.hex4()
				if !ok {
					p.bad = true
					return ""
				}
				if utf16.IsSurrogate(r) {
					r2 := rune(-1)
					if p.i+1 < len(p.s) && p.s[p.i] == '\\' && p.s[p.i+1] == 'u' {
						p.i += 2
						if r2, ok = p.hex4(); !ok {
							p.bad = true
							return ""
						}
					}
					if d := utf16.DecodeRune(r, r2); d != utf8.RuneError {
						r = d
					} else {
						p.bad = true
						return ""
					}
				}
				b = utf8.AppendRune(b, r)
			default:
				p.bad = true
				return ""
			}
		case c < utf8.RuneSelf:
			b = append(b, c)
			p.i++
		default:
			r, size := utf8.DecodeRuneInString(p.s[p.i:])
			if r == utf8.RuneError && size == 1 {
				p.bad = true
				return ""
			}
			b = append(b, p.s[p.i:p.i+size]...)
			p.i += size
		}
	}
	p.bad = true
	return ""
}

func (p *scanner) hex4() (rune, bool) {
	if p.i+4 > len(p.s) {
		return 0, false
	}
	n, err := strconv.ParseUint(p.s[p.i:p.i+4], 16, 32)
	if err != nil {
		return 0, false
	}
	p.i += 4
	return rune(n), true
}

// integer reads an integer with no fraction or exponent.
func (p *scanner) integer() int64 {
	p.ws()
	start := p.i
	if p.i < len(p.s) && p.s[p.i] == '-' {
		p.i++
	}
	digits := p.i
	for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
		p.i++
	}
	if p.i == digits || (p.s[digits] == '0' && p.i-digits > 1) {
		p.bad = true
		return 0
	}
	if p.i < len(p.s) {
		switch p.s[p.i] {
		case '.', 'e', 'E':
			p.bad = true
			return 0
		}
	}
	n, err := strconv.ParseInt(p.s[start:p.i], 10, 64)
	if err != nil {
		p.bad = true
	}
	return n
}

// skip passes over any JSON value.
func (p *scanner) skip() {
	switch p.peek() {
	case '{':
		p.i++
		if p.eat('}') {
			return
		}
		for !p.bad {
			p.str()
			if !p.eat(':') {
				p.bad = true
				return
			}
			p.skip()
			if p.eat('}') {
				return
			}
			if !p.eat(',') {
				p.bad = true
			}
		}
	case '[':
		p.i++
		if p.eat(']') {
			return
		}
		for !p.bad {
			p.skip()
			if p.eat(']') {
				return
			}
			if !p.eat(',') {
				p.bad = true
			}
		}
	case '"':
		p.str()
	case 'n', 't', 'f':
		for _, lit := range []string{"null", "true", "false"} {
			if len(p.s)-p.i >= len(lit) && p.s[p.i:p.i+len(lit)] == lit {
				p.i += len(lit)
				return
			}
		}
		p.bad = true
	default:
		p.number()
	}
}

// number passes over a number in JSON's grammar.
func (p *scanner) number() {
	p.ws()
	digits := func() int {
		start := p.i
		for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
			p.i++
		}
		return p.i - start
	}
	if p.i < len(p.s) && p.s[p.i] == '-' {
		p.i++
	}
	first := p.i
	n := digits()
	if n == 0 || (n > 1 && p.s[first] == '0') {
		p.bad = true
		return
	}
	if p.i < len(p.s) && p.s[p.i] == '.' {
		p.i++
		if digits() == 0 {
			p.bad = true
			return
		}
	}
	if p.i < len(p.s) && (p.s[p.i] == 'e' || p.s[p.i] == 'E') {
		p.i++
		if p.i < len(p.s) && (p.s[p.i] == '+' || p.s[p.i] == '-') {
			p.i++
		}
		if digits() == 0 {
			p.bad = true
		}
	}
}

// value reads one value object and hands its type and the scanner, placed
// at the start of the value member, to read. read must consume exactly the
// member's value.
func (p *scanner) value(read func(typ string)) {
	if !p.eat('{') {
		p.bad = true
		return
	}
	var typ string
	haveType, haveValue := false, false
	valueAt := -1
	for !p.bad {
		name := p.str()
		if !p.eat(':') {
			p.bad = true
			return
		}
		switch {
		case name == "type" && !haveType:
			typ, haveType = p.str(), true
		case name == "value" && !haveValue:
			haveValue = true
			p.ws()
			valueAt = p.i
			p.skip()
		default:
			p.bad = true
			return
		}
		if p.eat('}') {
			break
		}
		if !p.eat(',') {
			p.bad = true
			return
		}
	}
	if p.bad || !haveType || !haveValue {
		p.bad = true
		return
	}
	end := p.i
	p.i = valueAt
	read(typ)
	p.i = end
}

func (p *scanner) intValue() int64 {
	var n int64
	p.value(func(typ string) {
		if typ != "VInt" {
			p.bad = true
			return
		}
		n = p.integer()
	})
	return n
}

func (p *scanner) stringValue() string {
	var s string
	p.value(func(typ string) {
		if typ != "VString" {
			p.bad = true
			return
		}
		s = p.str()
	})
	return s
}

func (p *scanner) listValue() []int64 {
	var out []int64
	p.value(func(typ string) {
		if typ != "VList" || !p.eat('[') {
			p.bad = true
			return
		}
		out = []int64{}
		if p.eat(']') {
			return
		}
		for !p.bad {
			out = append(out, p.intValue())
			if p.eat(']') {
				return
			}
			if !p.eat(',') {
				p.bad = true
			}
		}
	})
	return out
}

// scanPayload fills r's fields for an operation of kind k from payload, and
// reports false when the payload is not in the scanner's shape.
func scanPayload(r *Row, k Kind, payload string) bool {
	p := &scanner{s: payload}
	if p.peek() == 0 {
		return true
	}
	if !p.eat('[') {
		return false
	}
	var key string
	var uid int64
	var list []int64
	hasKey, hasUID, hasList := false, false, false
	for n := 0; ; n++ {
		if n == 0 && p.eat(']') {
			break
		}
		switch {
		case p.peek() == '"':
			return false
		case r.Kind == Invocation && n == 1:
			key, hasKey = p.stringValue(), true
		case r.Kind == Invocation && n == 2 && k != Read:
			uid, hasUID = p.intValue(), true
		case r.Kind == Response && n == 0 && k != Write:
			list, hasList = p.listValue(), true
		default:
			p.skip()
		}
		if p.bad {
			return false
		}
		if p.eat(']') {
			break
		}
		if !p.eat(',') {
			return false
		}
	}
	p.ws()
	if p.i != len(p.s) {
		return false
	}
	r.Key, r.HasKey = key, hasKey
	r.UID, r.HasUID = uid, hasUID
	r.Value, r.HasValue = list, hasList
	return true
}
