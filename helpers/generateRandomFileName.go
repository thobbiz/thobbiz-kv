package helpers

import "strconv"

func GenerateFileName(index int) string {
	return string("store" + strconv.Itoa(index) + ".db")
}
