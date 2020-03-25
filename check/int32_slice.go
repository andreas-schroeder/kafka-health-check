package check

func contains(a []int32, el int32) bool {
	for _, e := range a {
		if e == el {
			return true
		}
	}
	return false
}

func delAll(a []int32, el int32) []int32 {
	ret := []int32{}
	for _, v := range a {
		if v != el {
			ret = append(ret, v)
		}
	}
	return ret
}
