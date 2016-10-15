package check

func contains(a []int32, el int32) bool {
	for _, e := range a {
		if e == el {
			return true
		}
	}
	return false
}

func indexOf(a []int32, el int32) (int, bool) {
	for i, e := range a {
		if e == el {
			return i, true
		}
	}
	return -1, false
}

func delAt(a []int32, i int) []int32 {
	copy(a[i:], a[i+1:])
	a[len(a)-1] = 0
	return a[:len(a)-1]
}

func delAll(a []int32, el int32) []int32 {
	for i, ok := indexOf(a, el); ok; {
		a = delAt(a, i)
	}
	return a
}
