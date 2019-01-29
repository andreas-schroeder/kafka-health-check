package check

func contains(a []int32, el int32) bool {
	for _, e := range a {
		if e == el {
			return true
		}
	}
	return false
}

func delAll(a []int32, el int32) (ret []int32) {
	for _, ael := range a {
		if ael != el {
			ret = append(ret, ael)
		}
	}
	return
}
