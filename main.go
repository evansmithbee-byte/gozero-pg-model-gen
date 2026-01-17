package main

import (
	"bytes"
	"database/sql"
	_ "embed"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"
	"time"

	_ "github.com/lib/pq"
)

//go:embed gen.gotpl
var genTpl string

//go:embed custom.gotpl
var customTpl string

//go:embed var.gotpl
var varTpl string

//go:embed base_field.gotpl
var baseFieldTpl string

type columnMeta struct {
	Name          string
	UDTName       string
	IsNullable    bool
	IsIdentity    bool
	ColumnDefault sql.NullString
	Comment       string
}

type tableMeta struct {
	Schema           string
	Table            string
	TypeName         string
	LowerTypeName    string
	FileBase         string
	PKColumns        []string
	PKParams         []param
	AutoSetColumns   []string
	Columns          []column
	InsertColumns    []column
	UpdateColumns    []column
	IndexedColumns   []column // [New] Columns that appear in any index
	UsedFieldTypes   map[string]bool
	Imports          []string
	GeneratedAtUTC   string
	GeneratorName    string
	GeneratorVersion string
}

type column struct {
	ColName string
	Field   string
	GoType  string
	Comment string
}

type param struct {
	Column string
	Name   string
	GoType string
	Field  string
}

func main() {
	var (
		url        = flag.String("url", "", "postgres url, e.g. postgres://user:pass@host:5432/db?sslmode=disable")
		schema     = flag.String("schema", "public", "schema name")
		table      = flag.String("table", "", "table name (without schema)")
		outDir     = flag.String("dir", "./internal/model", "output dir")
		pkg        = flag.String("package", "model", "go package name")
		withCustom = flag.Bool("with-custom", true, "generate *_model.go wrapper (if not exists)")
	)
	flag.Parse()

	if *url == "" || *table == "" {
		fmt.Fprintln(os.Stderr, "required: --url and --table")
		os.Exit(2)
	}

	// If package is default "model", use the last element of dir as package name
	p := *pkg
	if p == "model" && *outDir != "" {
		p = filepath.Base(*outDir)
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		die(err)
	}

	// Generate var.go
	varPath := filepath.Join(*outDir, "var.go")
	if _, err := os.Stat(varPath); os.IsNotExist(err) {
		if err := renderToFile(varTpl, map[string]any{
			"Package": p,
		}, varPath); err != nil {
			die(fmt.Errorf("generate var.go: %w", err))
		}
	} else if err != nil {
		die(fmt.Errorf("check var.go: %w", err))
	}

	// Generate base_field_gen.go
	baseFieldPath := filepath.Join(*outDir, "base_field_gen.go")
	if err := renderToFile(baseFieldTpl, map[string]any{
		"Package": p,
	}, baseFieldPath); err != nil {
		die(fmt.Errorf("generate base_field_gen.go: %w", err))
	}

	db, err := sql.Open("postgres", *url)
	if err != nil {
		die(err)
	}
	defer db.Close()

	tables := strings.Split(*table, ",")
	for _, t := range tables {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if err := generate(db, *schema, t, *outDir, p, *withCustom); err != nil {
			die(fmt.Errorf("table %s: %w", t, err))
		}
	}
}

func generate(db *sql.DB, schema, table, outDir, pkg string, withCustom bool) error {
	meta, err := introspect(db, schema, table)
	if err != nil {
		return err
	}

	meta.GeneratorName = "pgmodelgen"
	meta.GeneratorVersion = "0.1.0"
	meta.GeneratedAtUTC = time.Now().UTC().Format(time.RFC3339)

	genPath := filepath.Join(outDir, meta.FileBase+"_model_gen.go")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	if err := renderToFile(genTpl, map[string]any{
		"Package": pkg,
		"Meta":    meta,
	}, genPath); err != nil {
		return err
	}

	if withCustom {
		customPath := filepath.Join(outDir, meta.FileBase+"_model.go")
		if _, err := os.Stat(customPath); err == nil {
			// don't overwrite
		} else if os.IsNotExist(err) {
			if err := renderToFile(customTpl, map[string]any{
				"Package": pkg,
				"Meta":    meta,
			}, customPath); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}

func introspect(db *sql.DB, schema, table string) (tableMeta, error) {
	cols, err := readColumns(db, schema, table)
	if err != nil {
		return tableMeta{}, err
	}
	comments, err := readColumnComments(db, schema, table)
	if err != nil {
		return tableMeta{}, err
	}
	for i := range cols {
		if c, ok := comments[cols[i].Name]; ok {
			cols[i].Comment = c
		}
	}

	pkCols, err := readPrimaryKeyColumns(db, schema, table)
	if err != nil {
		return tableMeta{}, err
	}
	if len(pkCols) == 0 {
		pkCols, err = readUniqueKeyColumns(db, schema, table)
		if err != nil {
			return tableMeta{}, err
		}
	}
	if len(pkCols) == 0 {
		pkCols, err = readPartitionPrimaryKeyColumns(db, schema, table)
		if err != nil {
			return tableMeta{}, err
		}
	}
	if len(pkCols) == 0 {
		return tableMeta{}, fmt.Errorf("table %s.%s: missing primary key or unique constraint (pgmodelgen requires an identity; composite PK/Unique is supported)", schema, table)
	}

	typeName := toCamel(table)
	lowerTypeName := lowerFirst(typeName)

	// Decide auto-set columns (identity or nextval()).
	autoSet := map[string]bool{}
	for _, c := range cols {
		if c.IsIdentity {
			autoSet[c.Name] = true
			continue
		}
		if c.ColumnDefault.Valid && strings.HasPrefix(strings.ToLower(strings.TrimSpace(c.ColumnDefault.String)), "nextval(") {
			autoSet[c.Name] = true
		}
	}
	autoSetCols := make([]string, 0, len(autoSet))
	for k := range autoSet {
		autoSetCols = append(autoSetCols, k)
	}
	sort.Strings(autoSetCols)

	colModels := make([]column, 0, len(cols))
	insertCols := make([]column, 0, len(cols))
	updateCols := make([]column, 0, len(cols))
	pkSet := make(map[string]bool, len(pkCols))
	for _, p := range pkCols {
		pkSet[p] = true
	}

	// [New] Read all indexed columns for "Smart Covering Index" struct
	indexedColNames, err := readIndexedColumns(db, schema, table)
	if err != nil {
		return tableMeta{}, err
	}
	indexedSet := make(map[string]bool, len(indexedColNames))
	for _, n := range indexedColNames {
		indexedSet[n] = true
	}
	indexedCols := make([]column, 0, len(indexedColNames))

	for _, c := range cols {
		goType := pgTypeToGoType(c.UDTName)
		field := toCamel(c.Name)
		colModels = append(colModels, column{
			ColName: c.Name,
			Field:   field,
			GoType:  goType,
			Comment: c.Comment,
		})
		if indexedSet[c.Name] {
			indexedCols = append(indexedCols, column{
				ColName: c.Name,
				Field:   field,
				GoType:  goType,
				Comment: c.Comment,
			})
		}
		if !autoSet[c.Name] {
			insertCols = append(insertCols, column{
				ColName: c.Name,
				Field:   field,
				GoType:  goType,
				Comment: c.Comment,
			})
		}
		// For updates, don't update PK columns or auto-set columns.
		// Also exclude created_at (convention).
		if !autoSet[c.Name] && !pkSet[c.Name] && c.Name != "created_at" {
			updateCols = append(updateCols, column{
				ColName: c.Name,
				Field:   field,
				GoType:  goType,
				Comment: c.Comment,
			})
		}
	}

	// Primary key params (typed based on the column).
	colTypeByName := map[string]string{}
	usedFieldTypes := map[string]bool{}
	for _, c := range colModels {
		colTypeByName[c.ColName] = c.GoType
		usedFieldTypes[pgTypeToFieldType(c.GoType)] = true
	}
	pkParams := make([]param, 0, len(pkCols))
	for _, pk := range pkCols {
		pkParams = append(pkParams, param{
			Column: pk,
			Name:   toLowerCamel(pk),
			GoType: colTypeByName[pk],
			Field:  toCamel(pk),
		})
	}

	importSet := map[string]bool{
		`"context"`:      true,
		`"database/sql"`: true,
		`"fmt"`:          true,
		`"strings"`:      true,
		// `orderBy "gitea.allgoodgame.com/saas-backend/saas-common/model/order-by"`: true,
		`"github.com/Masterminds/squirrel"`:                  true,
		`"github.com/zeromicro/go-zero/core/stores/builder"`: true,
		`"github.com/zeromicro/go-zero/core/stores/sqlx"`:    true,
		`"github.com/zeromicro/go-zero/core/stringx"`:        true,
	}
	for _, c := range colModels {
		if c.GoType == "time.Time" {
			importSet[`"time"`] = true
		}
		if strings.Contains(c.GoType, "decimal.Decimal") {
			importSet[`"github.com/shopspring/decimal"`] = true
		}
		if strings.HasPrefix(c.GoType, "pq.") {
			importSet[`"github.com/lib/pq"`] = true
		}
	}
	imports := make([]string, 0, len(importSet))
	for imp := range importSet {
		imports = append(imports, imp)
	}
	sort.Strings(imports)

	return tableMeta{
		Schema:         schema,
		Table:          table,
		TypeName:       typeName,
		LowerTypeName:  lowerTypeName,
		FileBase:       table,
		PKColumns:      pkCols,
		PKParams:       pkParams,
		AutoSetColumns: autoSetCols,
		Columns:        colModels,
		InsertColumns:  insertCols,
		UpdateColumns:  updateCols,
		IndexedColumns: indexedCols,
		UsedFieldTypes: usedFieldTypes,
		Imports:        imports,
	}, nil
}

func pgTypeToFieldType(goType string) string {
	switch goType {
	case "int64":
		return "Int64"
	case "float64":
		return "Float64"
	case "string":
		return "String"
	case "bool":
		return "Bool"
	case "[]byte":
		return "Bytes"
	case "decimal.Decimal":
		return "Decimal"
	case "time.Time":
		return "Time"
	case "pq.Int64Array":
		return "Int64Array"
	case "pq.StringArray":
		return "StringArray"
	case "pq.Float64Array":
		return "Float64Array"
	case "pq.BoolArray":
		return "BoolArray"
	default:
		return "Generic"
	}
}

func readIndexedColumns(db *sql.DB, schema, table string) ([]string, error) {
	const q = `
select distinct a.attname
from pg_class t
join pg_namespace n on n.oid = t.relnamespace
join pg_index ix on t.oid = ix.indrelid
join pg_attribute a on a.attrelid = t.oid
where n.nspname = $1 
  and t.relname = $2
  and a.attnum = ANY(string_to_array(ix.indkey::text, ' ')::int2[])
order by a.attname`
	rows, err := db.Query(q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func readColumns(db *sql.DB, schema, table string) ([]columnMeta, error) {
	const q = `
select
  c.column_name,
  c.udt_name,
  c.is_nullable = 'YES' as is_nullable,
  c.is_identity = 'YES' as is_identity,
  c.column_default
from information_schema.columns c
where c.table_schema = $1
  and c.table_name = $2
order by c.ordinal_position`
	rows, err := db.Query(q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []columnMeta
	for rows.Next() {
		var m columnMeta
		if err := rows.Scan(&m.Name, &m.UDTName, &m.IsNullable, &m.IsIdentity, &m.ColumnDefault); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func readPrimaryKeyColumns(db *sql.DB, schema, table string) ([]string, error) {
	const q = `
select kcu.column_name
from information_schema.table_constraints tc
join information_schema.key_column_usage kcu
  on tc.constraint_name = kcu.constraint_name
  and tc.table_schema = kcu.table_schema
where tc.table_schema = $1
  and tc.table_name = $2
  and tc.constraint_type = 'PRIMARY KEY'
order by kcu.ordinal_position`
	rows, err := db.Query(q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func readUniqueKeyColumns(db *sql.DB, schema, table string) ([]string, error) {
	const q = `
select kcu.column_name
from information_schema.table_constraints tc
join information_schema.key_column_usage kcu
  on tc.constraint_name = kcu.constraint_name
  and tc.table_schema = kcu.table_schema
where tc.table_schema = $1
  and tc.table_name = $2
  and tc.constraint_type = 'UNIQUE'
  and tc.constraint_name = (
    select tc2.constraint_name
    from information_schema.table_constraints tc2
    where tc2.table_schema = $1
      and tc2.table_name = $2
      and tc2.constraint_type = 'UNIQUE'
    order by tc2.constraint_name
    limit 1
  )
order by kcu.ordinal_position`
	rows, err := db.Query(q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func readPartitionPrimaryKeyColumns(db *sql.DB, schema, table string) ([]string, error) {
	const q = `
select 
    kcu.column_name
from pg_inherits
join pg_class parent on pg_inherits.inhparent = parent.oid
join pg_class child on pg_inherits.inhrelid = child.oid
join pg_namespace n on parent.relnamespace = n.oid
join information_schema.table_constraints tc on tc.table_name = child.relname and tc.table_schema = n.nspname
join information_schema.key_column_usage kcu on tc.constraint_name = kcu.constraint_name and tc.table_schema = kcu.table_schema
where n.nspname = $1 
  and parent.relname = $2 
  and tc.constraint_type = 'PRIMARY KEY'
order by child.relname asc, kcu.ordinal_position asc
limit 10`
	rows, err := db.Query(q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	var seen = make(map[string]bool)
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		if seen[c] {
			break
		}
		cols = append(cols, c)
		seen[c] = true
	}
	return cols, rows.Err()
}

func readColumnComments(db *sql.DB, schema, table string) (map[string]string, error) {
	const q = `
select
  a.attname as column_name,
  coalesce(d.description, '') as description
from pg_catalog.pg_attribute a
join pg_catalog.pg_class c on a.attrelid = c.oid
join pg_catalog.pg_namespace n on c.relnamespace = n.oid
left join pg_catalog.pg_description d on d.objoid = c.oid and d.objsubid = a.attnum
where n.nspname = $1
  and c.relname = $2
  and a.attnum > 0
  and not a.attisdropped`
	rows, err := db.Query(q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, desc string
		if err := rows.Scan(&name, &desc); err != nil {
			return nil, err
		}
		out[name] = desc
	}
	return out, rows.Err()
}

func pgTypeToGoType(udt string) string {
	switch strings.ToLower(udt) {
	case "int2", "int4", "int8", "integer", "bigint", "smallint":
		return "int64"
	case "bool":
		return "bool"
	case "varchar", "text", "bpchar", "uuid":
		return "string"
	case "json", "jsonb":
		return "string"
	case "bytea":
		return "[]byte"
	case "float4", "float8":
		return "float64"
	case "numeric", "decimal":
		return "decimal.Decimal"
	case "timestamp", "timestamptz", "date":
		return "time.Time"
	case "_int2", "_int4", "_int8", "_integer", "_bigint", "_smallint":
		return "pq.Int64Array"
	case "_varchar", "_text", "_bpchar", "_uuid":
		return "pq.StringArray"
	case "_float4", "_float8":
		return "pq.Float64Array"
	case "_bool":
		return "pq.BoolArray"
	default:
		return "string"
	}
}

func toCamel(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '_' || r == '-' })
	for i := range parts {
		p := strings.ToLower(parts[i])
		if p == "id" {
			parts[i] = "Id"
			continue
		}
		if len(p) == 0 {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, "")
}

func toLowerCamel(s string) string {
	cc := toCamel(s)
	return lowerFirst(cc)
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

func renderToFile(tpl string, data any, outPath string) error {
	t, err := template.New("tpl").Funcs(template.FuncMap{
		"Join":    strings.Join,
		"Add":     func(a, b int) int { return a + b },
		"ToCamel": toCamel,
		"GoTypeToFieldType": func(goType string) string {
			switch goType {
			case "int64":
				return "Int64"
			case "float64":
				return "Float64"
			case "string":
				return "String"
			case "bool":
				return "Bool"
			case "[]byte":
				return "Bytes"
			case "decimal.Decimal":
				return "Decimal"
			case "time.Time":
				return "Time"
			case "pq.Int64Array":
				return "Int64Array"
			case "pq.StringArray":
				return "StringArray"
			case "pq.Float64Array":
				return "Float64Array"
			case "pq.BoolArray":
				return "BoolArray"
			default:
				return "Generic"
			}
		},
	}).Parse(tpl)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return err
	}

	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		// keep raw for easier debugging
		formatted = buf.Bytes()
	}
	return os.WriteFile(outPath, formatted, 0o644)
}
