package myhelper

import (
	"github.com/linlexing/datatable.go"
	"github.com/linlexing/dbhelper"
	"testing"
)

var (
	dns string = "root:llx123@/root?parseTime=true"
)

func GetTestTable() *dbhelper.DataTable {
	rev := dbhelper.NewDataTable("test")
	rev.AddColumn(dbhelper.NewDataColumn("pk1", datatable.String, 50, true))
	rev.AddColumn(dbhelper.NewDataColumn("pk2", datatable.Int64, 0, true))
	rev.AddColumn(dbhelper.NewDataColumn("str1", datatable.String, 300, false))
	rev.AddColumn(dbhelper.NewDataColumn("str2", datatable.String, 0, false))
	rev.AddColumn(dbhelper.NewDataColumn("num1", datatable.Float64, 0, true))
	rev.AddColumn(dbhelper.NewDataColumn("bool1", datatable.Bool, 0, true))
	rev.AddColumn(dbhelper.NewDataColumn("time1", datatable.Time, 0, false))
	rev.AddColumn(dbhelper.NewDataColumn("bys1", datatable.Bytea, 0, false))
	rev.SetPK("pk1", "pk2")
	return rev
}
func TestCreateTable(t *testing.T) {
	ahelper := NewMyHelper(dns)
	if err := ahelper.Open(); err != nil {
		t.Error(err)
	}
	defer ahelper.Close()
	if err := ahelper.UpdateStruct(nil, GetTestTable()); err != nil {
		t.Error(err)
	}
}
