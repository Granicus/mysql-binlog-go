package binlog

var tableMapCollectionInstance TableMapCollection

type TableMapCollection map[uint64]*TableMapEvent

func GetTableMapCollectionInstance() TableMapCollection {
	if tableMapCollectionInstance == nil {
		tableMapCollectionInstance = make(TableMapCollection)
	}

	return tableMapCollectionInstance
}
