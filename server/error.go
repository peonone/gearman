package server

type serverError struct {
	code string
	err  error
}

func (e *serverError) toArguments() []string {
	return []string{e.code, e.err.Error()}
}

func (e *serverError) Error() string {
	return e.err.Error()
}
