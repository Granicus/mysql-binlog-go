mysql-binlog-go
===============
Under development (DO NOT USE)

This library is designed for parsing mysql binary logs into structs that you can then manipulate in any way you want. I have no plans at this moment to support any versions of MySQL other than 5.6 (with v4 formatting and post 5.6.4 time variables).

usage
=====

    import (
      ...

      binlog "github.com/granicus/mysql-binlog-go"
    )

    ...

    log, err := binlog.OpenBinlog("mysql-bin.000001")
    if err != nil {
      panic(err)
    }

    for _, event := range log.Events() {
    	if event.Type() == binlog.WRITE_ROWS_EVENTv2 {
    		rowsEvent := event.Data().(*binlog.RowsEvent)

    		fmt.Println("Found some rows that were inserted:", rowsEvent.Rows)
    	}
    }