package myhelper

import (
	"bytes"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/linlexing/datatable.go"
	"github.com/linlexing/dbhelper"
	"strings"
	"text/template"
)

type MyMeta struct {
	*dbhelper.RootMeta
}

func init() {
	dbhelper.RegisterMetaHelper("mysql", NewMyMeta())
}

func NewMyMeta() *MyMeta {
	return &MyMeta{&dbhelper.RootMeta{}}
}
func (m *MyMeta) ParamPlaceholder(num int) string {
	return "?"
}
func (m *MyMeta) RegLike(value, strRegLike string) string {
	return value + " REGEXP " + strRegLike
}

func (m *MyMeta) TableExists(tablename string) (bool, error) {
	return m.DBHelper.Exists(fmt.Sprintf("SHOW TABLES LIKE '%s'", tablename))
}
func (m *MyMeta) DropPrimaryKey(tablename string) error {
	_, err := m.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY", tablename))
	return err
}
func (m *MyMeta) DropIndex(tablename, indexname string) error {
	_, err := m.DBHelper.Exec(fmt.Sprintf("DROP INDEX %s on %s", indexname, tablename))
	return err
}
func (m *MyMeta) getColumnDefine(dataType datatable.ColumnType, maxSize int, notNull bool, desc dbhelper.DBDesc) string {
	rev := ""
	switch dataType {
	case datatable.String:
		if maxSize > 0 {
			rev = fmt.Sprintf("VARCHAR(%d)", maxSize)
		} else {
			rev = "LONGTEXT"
		}
	case datatable.Bool:
		rev = "BIT(1)"
	case datatable.Int64:
		rev = "BIGINT"
	case datatable.Float64:
		rev = "DOUBLE"
	case datatable.Time:
		rev = "DATETIME"
	case datatable.Bytea:
		rev = "LONGBLOB"
	default:
		panic(fmt.Errorf("the type %s invalid", dataType))
	}
	if notNull {
		rev += " NOT NULL"
	} else {
		rev += " NULL"
	}

	if !desc.IsEmpty() {
		rev += " COMMENT " + m.StringExpress(desc.String())
	}
	return rev
}
func (m *MyMeta) StringExpress(value string) string {
	var rev bytes.Buffer

	for _, c := range value {
		if c == 0 {
			rev.WriteString(`\0`)
		} else if c == 26 {
			rev.WriteString(`\Z`)
		} else {
			switch c {
			case '\'':
				rev.WriteString(`\'`)
			case '"':
				rev.WriteString(`\"`)
			case '\b':
				rev.WriteString(`\b`)
			case '\n':
				rev.WriteString(`\n`)
			case '\r':
				rev.WriteString(`\r`)
			case '\t':
				rev.WriteString(`\t`)
			case '\\':
				rev.WriteString(`\\`)
			default:
				rev.WriteRune(c)
			}
		}
	}
	return "'" + rev.String() + "'"
}
func (m *MyMeta) AlterColumn(tablename string, oldColumn, newColumn *dbhelper.TableColumn) error {
	if oldColumn.Name != newColumn.Name {
		_, err := m.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s %s", tablename, oldColumn.Name, newColumn.Name, m.getColumnDefine(newColumn.Type, newColumn.MaxSize, newColumn.NotNull, newColumn.Desc)))
		return err
	} else {
		_, err := m.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", tablename, newColumn.Name, m.getColumnDefine(newColumn.Type, newColumn.MaxSize, newColumn.NotNull, newColumn.Desc)))
		return err
	}
}
func (m *MyMeta) AlterTableDesc(tablename string, desc dbhelper.DBDesc) error {
	_, err := m.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s COMMENT %s", tablename, m.StringExpress(desc.String())))
	return err
}
func (m *MyMeta) AlterIndex(tablename, indexname string, oldIndex, newIndex *dbhelper.Index) error {
	if err := m.DropIndex(tablename, indexname); err != nil {
		return err
	}
	if err := m.CreateIndex(tablename, indexname, newIndex.Columns, newIndex.Unique, newIndex.Desc); err != nil {
		return err
	}
	return nil
}
func (m *MyMeta) CreateIndex(tableName, indexName string, columns []string, unique bool, desc dbhelper.DBDesc) error {
	uniqueStr := ""
	if unique {
		uniqueStr = "UNIQUE"
	}
	commentStr := ""
	if !desc.IsEmpty() {
		commentStr = fmt.Sprintf("\nCOMMENT %s", m.StringExpress(desc.String()))
	}
	_, err := m.DBHelper.Exec(fmt.Sprintf("CREATE %sINDEX %s ON %s(%s)%s", uniqueStr, indexName, tableName, strings.Join(columns, ","), commentStr))
	return err
}
func (m *MyMeta) CreateTable(table *dbhelper.DataTable) error {
	creates := make([]string, table.ColumnCount())
	for i, c := range table.Columns {
		creates[i] = fmt.Sprintf("%s %s", c.Name, m.getColumnDefine(c.DataType, c.MaxSize, c.NotNull, c.Desc))
	}
	if table.HasPrimaryKey() {
		creates = append(creates, fmt.Sprintf("PRIMARY KEY(%s)", strings.Join(table.PK, ",")))
	}
	commentStr := ""
	if !table.Desc.IsEmpty() {
		commentStr = fmt.Sprintf("\nCOMMENT %s", m.StringExpress(table.Desc.String()))
	}
	if table.Temporary {
		_, err := m.DBHelper.Exec(fmt.Sprintf("CREATE TEMPORARY TABLE %s(\n%s\n)%s", table.TableName, strings.Join(creates, ","), commentStr))
		return err
	} else {
		_, err := m.DBHelper.Exec(fmt.Sprintf("CREATE TABLE %s(\n%s\n)%s", table.TableName, strings.Join(creates, ","), commentStr))
		return err
	}
}
func (m *MyMeta) AddColumn(tablename string, column *dbhelper.TableColumn) error {
	_, err := m.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tablename, column.Name, m.getColumnDefine(column.Type, column.MaxSize, column.NotNull, column.Desc)))
	return err
}
func (m *MyMeta) AddPrimaryKey(tablename string, pks []string) error {
	_, err := m.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s ADD PRIAMRY KEY(%s)", tablename, strings.Join(pks, ",")))
	return err
}
func (m *MyMeta) GetTableDesc(tablename string) (dbhelper.DBDesc, error) {
	rev, err := m.DBHelper.QueryOne(`
SELECT table_comment
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema=SCHEMA()
AND table_name=$1`, tablename)
	if err != nil {
		return nil, err
	}
	if rev == nil || rev.(string) == "" {
		return dbhelper.DBDesc{}, nil
	} else {
		v := dbhelper.DBDesc{}
		v.Parse(rev.(string))
		return v, nil
	}
}
func (m *MyMeta) GetIndexes(tablename string) ([]*dbhelper.TableIndex, error) {
	table, err := m.DBHelper.GetData(`
SELECT index_name,
	max(non_unique) as non_unique,
	max(index_comment) as index_desc,
	GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns
FROM information_schema.statistics
WHERE table_schema =schema() and
	table_name = $1 and
	index_name <>'PRIMARY'
GROUP BY table_name,index_name`, tablename)
	if err != nil {
		return nil, err
	}
	rev := make([]*dbhelper.TableIndex, table.RowCount())
	for i := 0; i < table.RowCount(); i++ {
		row := table.Row(i)
		rev[i] = &dbhelper.TableIndex{}
		rev[i].Name = row["index_name"].(string)
		rev[i].Columns = strings.Split(row["columns"].(string), ",")
		if row["non_unique"].(int64) == 1 {
			rev[i].Unique = false
		} else {
			rev[i].Unique = true
		}
		rev[i].Desc = dbhelper.DBDesc{}
		if row["index_desc"] != nil {
			rev[i].Desc.Parse(row["index_desc"].(string))
		}
	}
	return rev, nil
}
func (m *MyMeta) GetColumns(tablename string) ([]*dbhelper.TableColumn, error) {
	table, err := m.DBHelper.GetData(`
SELECT column_name,data_type,character_maximum_length,numeric_precision,is_nullable,column_comment
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA=SCHEMA() AND
	table_name=$1`, tablename)
	if err != nil {
		return nil, err
	}
	rev := make([]*dbhelper.TableColumn, table.RowCount())
	for i := 0; i < table.RowCount(); i++ {
		row := table.Row(i)
		rev[i] = &dbhelper.TableColumn{}
		rev[i].Name = row["column_name"].(string)
		switch row["data_type"].(string) {
		case "varchar":
			rev[i].Type = datatable.String
			rev[i].MaxSize = int(row["character_maximum_length"].(int64))
		case "longtext":
			rev[i].Type = datatable.String
		case "datetime":
			rev[i].Type = datatable.Time
		case "double":
			rev[i].Type = datatable.Float64
		case "bigint":
			rev[i].Type = datatable.Int64
		case "longblob":
			rev[i].Type = datatable.Bytea
		case "bit":
			if row["numeric_precision"].(int64) == int64(1) {
				rev[i].Type = datatable.Bool
			} else {
				return nil, fmt.Errorf("the column %q type %s invalid", row["column_name"], row["data_type"])
			}
		default:
			return nil, fmt.Errorf("the column %q type %s invalid", row["column_name"], row["data_type"])
		}
		if row["is_nullable"].(string) == "YES" {
			rev[i].NotNull = false
		} else {
			rev[i].NotNull = true
		}
		if row["column_comment"] != nil || row["column_comment"].(string) != "" {
			desc := dbhelper.DBDesc{}
			desc.Parse(row["column_comment"].(string))
			rev[i].Desc = desc
		}
	}
	return rev, nil
}
func (m *MyMeta) GetPrimaryKeys(tablename string) ([]string, error) {
	pks, err := m.DBHelper.QueryOne(`
SELECT
	GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns
FROM information_schema.statistics
WHERE table_schema =schema() and
	table_name = $1 and
	index_name ='PRIMARY'`, tablename)
	if err != nil {
		return nil, err
	}
	return strings.Split(pks.(string), ","), nil
}
func (m *MyMeta) Merge(dest, source string, colNames []string, pkColumns []string, autoRemove bool, sqlWhere string) error {
	if len(pkColumns) == 0 {
		return fmt.Errorf("the primary keys is empty")
	}
	if len(colNames) == 0 {
		return fmt.Errorf("the columns is empty")
	}
	tmp := template.New("sql")
	tmp.Funcs(template.FuncMap{
		"Join": func(value []string, sep, prefix string) string {
			if prefix == "" {
				return strings.Join(value, sep)
			} else {
				rev := make([]string, len(value))
				for i, v := range value {
					rev[i] = prefix + v
				}
				return strings.Join(rev, sep)
			}
		},
		"First": func(value []string) string {
			return value[0]
		},
	})
	tmp, err := tmp.Parse(`
{{if .autoRemove}}
DELETE dest FROM {{.destTable}} dest WHERE{{if ne .sqlWhere ""}}
    ({{.sqlWhere}}) AND {{end}}
    NOT EXISTS(
        SELECT 1 FROM {{.sourceTable}} src WHERE{{range $idx,$colName :=.pkColumns}}
            {{if gt $idx 0}}AND {{end}}dest.{{$colName}}=src.{{$colName}}{{end}}
    );
go
{{end}}
INSERT INTO {{.destTable}}(
    {{Join .colNames ",\n    " ""}}
)
SELECT
    {{Join .colNames ",\n    " "src."}}
FROM
    {{.sourceTable}} src
ON DUPLICATE KEY UPDATE{{range $idx,$colName :=.updateColumns}}
	{{if gt $idx 0}},{{end}}{{$colName}}=src.{{$colName}}
	{{end}};`)
	if err != nil {
		return err
	}
	var b bytes.Buffer
	//primary key not update
	updateColumns := []string{}

	for _, v := range colNames {
		bFound := false
		for _, pv := range pkColumns {
			if v == pv {
				bFound = true
				break
			}
		}
		if !bFound {
			updateColumns = append(updateColumns, v)
		}
	}

	param := map[string]interface{}{
		"destTable":     dest,
		"sourceTable":   source,
		"updateColumns": updateColumns,
		"colNames":      colNames,
		"autoRemove":    autoRemove,
		"sqlWhere":      sqlWhere,
		"pkColumns":     pkColumns,
	}
	if err := tmp.Execute(&b, param); err != nil {
		return err
	}
	err = m.DBHelper.GoExec(b.String())
	return err
}
