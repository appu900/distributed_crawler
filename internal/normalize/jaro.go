package normalize

// JaroWinkler returns the Jaro-Winkler similarity of a and b in [0,1].
func JaroWinkler(a, b string) float64 {
	ra, rb := []rune(a), []rune(b)
	if len(ra) == 0 && len(rb) == 0 {
		return 1
	}
	if len(ra) == 0 || len(rb) == 0 {
		return 0
	}
	window := max(len(ra), len(rb))/2 - 1
	if window < 0 {
		window = 0
	}
	ma := make([]bool, len(ra))
	mb := make([]bool, len(rb))
	matches := 0
	for i := range ra {
		lo, hi := max(0, i-window), min(len(rb)-1, i+window)
		for j := lo; j <= hi; j++ {
			if !mb[j] && ra[i] == rb[j] {
				ma[i], mb[j] = true, true
				matches++
				break
			}
		}
	}
	if matches == 0 {
		return 0
	}
	transpositions, k := 0, 0
	for i := range ra {
		if !ma[i] {
			continue
		}
		for !mb[k] {
			k++
		}
		if ra[i] != rb[k] {
			transpositions++
		}
		k++
	}
	m := float64(matches)
	jaro := (m/float64(len(ra)) + m/float64(len(rb)) + (m-float64(transpositions)/2)/m) / 3
	prefix := 0
	for prefix < min(4, min(len(ra), len(rb))) && ra[prefix] == rb[prefix] {
		prefix++
	}
	return jaro + float64(prefix)*0.1*(1-jaro)
}

// SamePerson reports whether two folded person names plausibly denote the same
// individual: high Jaro-Winkler similarity, or the same first and last token
// with only middle names or initials differing.
func SamePerson(a, b string, threshold float64) bool {
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	ta, tb := fields(a), fields(b)
	if len(ta) >= 2 && len(tb) >= 2 {
		lastA, lastB := ta[len(ta)-1], tb[len(tb)-1]
		if lastA != lastB && JaroWinkler(lastA, lastB) < 0.9 {
			// Different surnames are different people regardless of how
			// similar the full strings look ("Jane Doe" vs "Jane Dow").
			return false
		}
		if lastA == lastB && firstCompatible(ta[0], tb[0]) && (len(ta) != len(tb) || len(ta) == 2) {
			return true
		}
	}
	return JaroWinkler(a, b) >= threshold
}

func firstCompatible(a, b string) bool {
	if a == b {
		return true
	}
	// Initial vs full first name: "j" matches "jane".
	if len(a) == 1 && len(b) > 0 && a[0] == b[0] {
		return true
	}
	if len(b) == 1 && len(a) > 0 && a[0] == b[0] {
		return true
	}
	return false
}

func fields(s string) []string {
	var out []string
	start := -1
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ' ' {
			if start >= 0 {
				out = append(out, s[start:i])
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	return out
}
