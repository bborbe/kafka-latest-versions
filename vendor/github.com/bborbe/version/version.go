package version

import (
	"github.com/bborbe/stringutil"
	"regexp"
	"strconv"
)

type Version string

func (v Version) String() string {
	return string(v)
}

func (v Version) Less(version Version) bool {
	p1 := regSplit(v.String())
	p2 := regSplit(version.String())
	for i := 0; i < len(p1) && i < len(p2); i++ {
		if partLess(p1[i], p2[i]) {
			return true
		}
		if partLess(p2[i], p1[i]) {
			return false
		}
	}
	if len(p1) < len(p2) {
		_, err := strconv.Atoi(p2[len(p1)])
		return err == nil
	}
	if len(p1) > len(p2) {
		_, err := strconv.Atoi(p1[len(p2)])
		return err != nil
	}
	return false
}

func partLess(s1 string, s2 string) bool {
	n1, e1 := strconv.Atoi(s1)
	n2, e2 := strconv.Atoi(s2)
	if e1 == nil && e2 == nil {
		if n1 < n2 {
			return true
		}
	} else {
		if stringutil.StringLess(s1, s2) {
			return true
		}
	}
	return false
}

func regSplit(text string) []string {
	reg := regexp.MustCompile("[-_.]")
	indexes := reg.FindAllStringIndex(text, -1)
	laststart := 0
	result := make([]string, len(indexes)+1)
	for i, element := range indexes {
		result[i] = text[laststart:element[0]]
		laststart = element[1]
	}
	result[len(indexes)] = text[laststart:len(text)]
	return result
}
