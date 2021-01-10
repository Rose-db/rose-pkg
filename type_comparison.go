package rose

import "strings"

type str string
type integer int
type floating float64
type boolean bool

func (s str) compare(p string, t comparisonType) bool {
	if t == equality {
		return strings.Compare(string(s), p) == 0
	} else if t == inequality {
		return strings.Compare(string(s), p) != 0
	} else if t == less {
		return strings.Compare(string(s), p) == -1
	} else if t == more {
		return strings.Compare(string(s), p) != 1
	} else if t == lessEqual {
		return strings.Compare(string(s), p) == -1 || strings.Compare(string(s), p) == 0
	} else if t == moreEqual {
		return strings.Compare(string(s), p) == 1 || strings.Compare(string(s), p) == 0
	}

	return false
}

func (s integer) compare(p int, t comparisonType) bool {
	if t == equality {
		return int(s) == p
	} else if t == inequality {
		return int(s) != p
	} else if t == less {
		return int(s) < p
	} else if t == more {
		return int(s) > p
	} else if t == lessEqual {
		return int(s) <= p
	} else if t == moreEqual {
		return int(s) >= p
	}

	return false
}

func (s floating) compare(p float64, t comparisonType) bool {
	if t == equality {
		return float64(s) == p
	} else if t == inequality {
		return float64(s) != p
	} else if t == less {
		return float64(s) < p
	} else if t == more {
		return float64(s) > p
	} else if t == lessEqual {
		return float64(s) <= p
	} else if t == moreEqual {
		return float64(s) >= p
	}

	return false
}

func (s boolean) compare(p bool, t comparisonType) bool {
	if t == equality {
		return bool(s) == p
	} else if t == inequality {
		return bool(s) != p
	}

	return false
}
