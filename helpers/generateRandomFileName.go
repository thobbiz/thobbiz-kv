package helpers

import "strconv"

func GenerateRandomFileName(index int) string {
	return string("store" + strconv.Itoa(index))
}
