package version

type VersionByName []Version

func (v VersionByName) Len() int {
	return len(v)
}
func (v VersionByName) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}
func (v VersionByName) Less(i, j int) bool {
	return LessThan(v[i], v[j])
}

func LessThan(a Version, b Version) bool {
	return a.Less(b)
}

func GreaterThan(a Version, b Version) bool {
	return LessThan(b, a)
}

func LessEqual(a Version, b Version) bool {
	return !GreaterThan(a, b)
}

func GreaterEqual(a Version, b Version) bool {
	return !LessThan(a, b)
}

func Equal(a Version, b Version) bool {
	return !LessThan(a, b) && !GreaterThan(a, b)
}
