package binlog

import (
	"fmt"
)

func main() {
	binlog, err := OpenBinlog("/usr/local/var/mysql/mysql-bin.000070")

	fmt.Println("Error: ", err)

	fmt.Println("NextEvent()")

	count := 0

	for {
		binlog.NextEvent()

		count++
		fmt.Println("Events read:", count)
	}
}
