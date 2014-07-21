package myhelper

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/linlexing/dbhelper"
)

func NewMyHelper(dataSource string) *dbhelper.DBHelper {
	return dbhelper.NewDBHelper("mysql", dataSource, NewMyMeta())
}
