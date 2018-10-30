package utils

func UniqueStringSlice(inp []string) []string {
	ret := []string{}
	for _, candidate := range inp {
		duplicate := false
		for _, member := range ret {
			if candidate == member {
				duplicate = true
				break
			}
		}
		if !duplicate {
			ret = append(ret, candidate)
		}
	}
	return ret
}
