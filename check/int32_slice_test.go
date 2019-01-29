package check

import (
	"reflect"
	"testing"
)

// contains() tests

func Test_Contains_ReturnsFalse(t *testing.T) {
	if contains([]int32{1, 2, 3}, 4) == true {
		t.Error()
	}
}

func Test_Contains_ReturnsTrue(t *testing.T) {
	if contains([]int32{1, 2, 3}, 2) == false {
		t.Error()
	}
}

// delAll() tests

func Test_DelAll_Single(t *testing.T) {
	if !reflect.DeepEqual(delAll([]int32{1, 2, 3}, 2), []int32{1, 3}) {
		t.Error()
	}
}

func Test_DelAll_Multiple(t *testing.T) {
	if !reflect.DeepEqual(delAll([]int32{1, 2, 3, 2}, 2), []int32{1, 3}) {
		t.Error()
	}
}

func Test_DelAll_Missing(t *testing.T) {
	if !reflect.DeepEqual(delAll([]int32{1, 2, 3}, 4), []int32{1, 2, 3}) {
		t.Error()
	}
}

func Test_DelAll_Front(t *testing.T) {
	if !reflect.DeepEqual(delAll([]int32{1, 2, 3}, 1), []int32{2, 3}) {
		t.Error()
	}
}

func Test_DelAll_Back(t *testing.T) {
	if !reflect.DeepEqual(delAll([]int32{1, 2, 3}, 3), []int32{1, 2}) {
		t.Error()
	}
}
