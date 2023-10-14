package grecho

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func mapSlice[In any, Out any](s []In, fn func(v In) Out) []Out {
	if s == nil {
		return nil
	}
	s2 := make([]Out, len(s))
	for i := range s {
		s2[i] = fn(s[i])
	}
	return s2
}

func mapSliceWithIndex[In any, Out any](s []In, fn func(i int, v In) Out) []Out {
	if s == nil {
		return nil
	}
	s2 := make([]Out, len(s))
	for i := range s {
		s2[i] = fn(i, s[i])
	}
	return s2
}

func anifySlice[T any](s []T) []any {
	return mapSlice(
		s, func(v T) any {
			return any(v)
		},
	)
}
