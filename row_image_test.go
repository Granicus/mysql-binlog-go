package binlog

/*
import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

const (
	mysqlBinlogLocation string = "/usr/local/var/mysql/"
)

var (
	testDatabaseConnectionString string = "fudd:wabbit-season@tcp(127.0.0.1:3306)"
)

func sliceDelete(slice []string, value string) []string {
	for i, s := range slice {
		if s == value {
			return append(slice[:i], slice[i+1:]...)
		}
	}

	return slice
}

func openTestDatabase(t *testing.T) *sql.DB {
	db, err := sql.Open("mysql", fmt.Sprintf(testDatabaseConnectionString+"/fuddtest"))
	checkTest(t, err)
	return db
}

type byBinlogNumber []string

func (a byBinlogNumber) getBinlogNumber(i int) int {
	parts := strings.Split(a[i], ".")
	if len(parts) != 2 {
		fmt.Println("Failed to parse binlog number: invalid split sections")
		return 0
	}

	n, err := strconv.ParseInt(parts[1], 10, 0)
	if err != nil {
		fmt.Println("Failed to parse binlog number:", err.Error())
		return 0
	}

	return int(n)
}

func (a byBinlogNumber) Len() int           { return len(a) }
func (a byBinlogNumber) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byBinlogNumber) Less(i, j int) bool { return a.getBinlogNumber(i) < a.getBinlogNumber(j) }

func sortedMysqlBinlogFilenames() []string {
	filenames, err := filepath.Glob(filepath.Join(mysqlBinlogLocation, "mysql-bin.*"))
	if err != nil {
		return []string{}
	}

	filenames = sliceDelete(filenames, filepath.Join(mysqlBinlogLocation, "mysql-bin.index"))

	sort.Sort(byBinlogNumber(filenames))
	return filenames
}

func lastMysqlBinlogFilename(t *testing.T) string {
	filenames := sortedMysqlBinlogFilenames()
	if len(filenames) == 0 {
		t.Error("No binlogs found")
	}

	return filenames[len(filenames)-1]
}

func grabLastBinlogEvent(t *testing.T) []byte {
	binlogFilename := lastMysqlBinlogFilename(t)

	binlogFile, err := os.Open(binlogFilename)
	checkTest(t, err)
	defer binlogFile.Close()

	stat, err := binlogFile.Stat()
	checkTest(t, err)

	_, err = binlogFile.Seek(4, 0)
	checkTest(t, err)

	for {
		startPosition, err := binlogFile.Seek(0, 1)
		checkTest(t, err)

		header := deserializeEventHeader(binlogFile)

		if int64(header.NextPosition) >= stat.Size() {
			_, err = binlogFile.Seek(startPosition, 0)
			checkTest(t, err)

			b := make([]byte, int64(header.NextPosition) - startPosition)
			_, err := binlogFile.Read(b)
			checkTest(t, err)

			return b
		}

		_, err = binlogFile.Seek(int64(header.NextPosition), 1)
		checkTest(t, err)
	}
}

func TestDeserializeRowImageCell(t *testing.T) {
	createTable := `
		CREATE TABLE all_mysql_types
		(
			tiny_col tinyint,
			small_col smallint,
			medium_col mediumint,
			int_col int,
			big_col bigint,
			decimal_col decimal(5, 2),
			numeric_col numeric,
			float_col float,
			double_col double,
			bit_col bit(8),
			date_col date,
			datetime_col datetime,
			timestamp_col timestamp,
			time_col time,
			year2_col year(2),
			year4_col year(4),
			char_col char(5),
			varchar_col varchar(255),
			binary_col binary(8),
			varbinary_col varbinary(8),
			blob_col blob,
			text_col text,
			enum_col enum('1', '2', '3'),
			set_col set('one', 'two', 'three')
		)`

	insertIntoTable := `
		INSERT INTO all_mysql_types
		(
			tiny_col,
			small_col,
			medium_col,
			int_col,
			big_col,
			decimal_col,
			numeric_col,
			float_col,
			double_col,
			bit_col,
			date_col,
			datetime_col,
			timestamp_col,
			time_col,
			year2_col,
			year4_col,
			char_col,
			varchar_col,
			binary_col,
			varbinary_col,
			blob_col,
			text_col,
			enum_col,
			set_col
		)
		VALUES
		(
			5,
			5,
			5,
			5,
			5,
			5.5,
			5.5,
			5.5,
			5.5,
			b'01010101',
			'1994-06-04',
			'1994-06-04 00:00:00',
			'1994-06-04 00:00:00',
			microsecond('12:00:30.05'),
			'94',
			'1994',
			'hello',
			'hello',
			'a8fe19',
			'a8fe19',
			'hello',
			'hello',
			'1',
			'1'
		)`

	db := openTestDatabase(t)

	_, err := db.Query(createTable)
	checkTest(t, err)

	_, err = db.Query(insertIntoTable)
	checkTest(t, err)

	db.Close()

	defer func() {
		db = openTestDatabase(t)

		_, err = db.Query("DROP TABLE all_mysql_types")
		checkTest(t, err)

		db.Close()
	}()

	lastEventBytes := grabLastBinlogEvent(t)

	lastEventReader := bytes.NewReader(lastEventBytes)
	header := deserializeEventHeader(lastEventReader)

	fmt.Println(header)

	assert.Contains(t, []MysqlBinlogEventType{
		WRITE_ROWS_EVENTv0, WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2,
		UPDATE_ROWS_EVENTv0, UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2,
		DELETE_ROWS_EVENTv0, DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2,
	  }, header.Type, "But header is not of expected type")

	event := new(Event)
	event.Header = header

	data := header.DataDeserializer().Deserialize(lastEventReader, event)
	var _ = data // compiler hack

	// a bunch of data assertions
}
*/
