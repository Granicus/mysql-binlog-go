package date

import (
	"fmt"
	"strconv"
)

func filledByteSlice(defaultByte byte, count int) []byte {
	b := make([]byte, count)
	for i := range b {
		b[i] = defaultByte
	}

	return b
}

func padStringNumber(str string, count int) string {
	return string(filledByteSlice('0', count-len(str))) + str
}

type MysqlDate struct {
	year  int
	month int
	day   int
}

func NewMysqlDate(year, month, day int) MysqlDate {
	return MysqlDate{
		year:  year,
		month: month,
		day:   day,
	}
}

func (date MysqlDate) Year() string {
	return padStringNumber(strconv.FormatInt(int64(date.year), 10), 4)
}

func (date MysqlDate) Month() string {
	return padStringNumber(strconv.FormatInt(int64(date.month), 10), 2)
}

func (date MysqlDate) Day() string {
	return padStringNumber(strconv.FormatInt(int64(date.day), 10), 2)
}

func (date MysqlDate) String() string {
	return fmt.Sprintf("%v-%v-%v", date.Year(), date.Month(), date.Day())
}

type MysqlTime struct {
	hour   int
	minute int
	second int
}

func NewMysqlTime(hour, minute, second int) MysqlTime {
	return MysqlTime{
		hour:   hour,
		minute: minute,
		second: second,
	}
}

func (timestamp MysqlTime) Hour() string {
	return padStringNumber(strconv.FormatInt(int64(timestamp.hour), 10), 2)
}

func (timestamp MysqlTime) Minute() string {
	return padStringNumber(strconv.FormatInt(int64(timestamp.minute), 10), 2)
}

func (timestamp MysqlTime) Second() string {
	return padStringNumber(strconv.FormatInt(int64(timestamp.second), 10), 2)
}

func (timestamp MysqlTime) String() string {
	return fmt.Sprintf("%v:%v:%v", timestamp.Hour(), timestamp.Minute(), timestamp.Second())
}

type MysqlDatetime struct {
	MysqlDate
	MysqlTime
}

func NewMysqlDatetime(year, month, day, hour, minute, second int) MysqlDatetime {
	return MysqlDatetime{
		MysqlDate: NewMysqlDate(year, month, day),
		MysqlTime: NewMysqlTime(hour, minute, second),
	}
}

func (dateTime MysqlDatetime) String() string {
	return fmt.Sprintf("%v %v", dateTime.MysqlDate.String(), dateTime.MysqlTime.String())
}
