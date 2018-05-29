package server

import (
	"time"
)

type supportFunctions map[string]time.Duration

func newSupportFunctions() supportFunctions {
	return supportFunctions(make(map[string]time.Duration))
}

func (sf supportFunctions) timeout(function string) time.Duration {
	return sf[function]
}

func (sf supportFunctions) support(function string) bool {
	_, ok := sf[function]
	return ok
}

func (sf supportFunctions) canDo(function string, timeout time.Duration) {
	sf[function] = timeout
}

func (sf supportFunctions) cantDo(function string) {
	delete(sf, function)
}

func (sf supportFunctions) reset() {
	for function := range sf {
		delete(sf, function)
	}
}

func (sf supportFunctions) toSlice() []string {
	ret := make([]string, len(sf))
	i := 0
	for function := range sf {
		ret[i] = function
		i++
	}
	return ret
}
