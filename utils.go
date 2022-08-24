package gostan

func stringToMap(s string, header [][]byte) map[string]interface{} {

	dat := make(map[string]interface{})

	res := []string{}
	var beg int
	var inString bool

	for i := 0; i < len(s); i++ {
		if s[i] == ',' && !inString {
			res = append(res, s[beg:i])
			beg = i + 1
		} else if s[i] == '"' {
			if !inString {
				inString = true
			} else if i > 0 && s[i-1] != '\\' {
				inString = false
			}
		}
	}
	res = append(res, s[beg:])
	if len(header) != len(res) {
		return nil
	}
	for i, key := range header {
		dat[string(key)] = res[i]
	}

	return dat
}
