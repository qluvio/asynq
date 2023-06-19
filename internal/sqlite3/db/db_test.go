package db

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/hibiken/asynq/internal/sqlite3/command"
	"github.com/hibiken/asynq/internal/sqlite3/db/testdata/chinook"
	"github.com/hibiken/asynq/internal/sqlite3/encoding"
)

func Test_ReturningClause(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer ignoreErr(db.Close)

	r, err := db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	rqs, err := db.Request(&command.Request{
		Statements: []*command.Statement{
			{
				Sql:       `INSERT INTO foo(id, name) VALUES(1, "fiona") RETURNING id`,
				Returning: true,
			},
		},
	}, true)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if len(rqs) != 1 {
		t.Fatalf("expected one query response")
	}
	rq := rqs[0]
	if rq.GetError() != "" {
		t.Fatalf("expected no error, got %s", rq.GetError())
	}

	rows := rq.GetQ()
	if rows == nil {
		t.Fatalf("expected rows result")
	}

	if len(rows.GetColumns()) != 1 && rows.GetColumns()[0] != "id" {
		t.Fatalf("expected one 'id' column")
	}
	if len(rows.GetTypes()) != 1 && rows.GetTypes()[0] != "integer" {
		t.Fatalf("expected one 'integer' type")
	}
	if len(rows.GetValues()) != 1 && len(rows.GetValues()[0].GetParameters()) != 1 && rows.GetValues()[0].GetParameters()[0].GetInt64() != int64(1) {
		t.Fatalf("expected returned value 1")
	}
}

func Test_WriteOnQueryInMemDatabase(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer ignoreErr(db.Close)

	r, err := db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(`INSERT INTO foo(id, name) VALUES(1, "fiona")`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	ro, err := db.QueryStringStmt(`INSERT INTO foo(id, name) VALUES(2, "fiona")`)
	if err != nil {
		t.Fatalf("error attempting read-only write test: %s", err)
	}
	if exp, got := `[{"error":"attempt to change database via query operation"}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err = db.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":[""],"values":[[1]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_ConcurrentQueriesInMemory(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer ignoreErr(db.Close)

	r, err := db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	for i := 0; i < 5000; i++ {
		r, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
		if err != nil {
			t.Fatalf("failed to insert record: %s", err.Error())
		}
		if exp, got := fmt.Sprintf(`[{"last_insert_id":%d,"rows_affected":1}]`, i+1), asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ro, err := db.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
			if err != nil {
				t.Logf("failed to query table: %s", err.Error())
			}
			if exp, got := `[{"columns":["COUNT(*)"],"types":[""],"values":[[5000]]}]`, asJSON(ro); exp != got {
				t.Logf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
			}
		}()
	}
	wg.Wait()
}

// Test_ConnectionIsolation test that ISOLATION behavior of on-disk databases doesn't
// change unexpectedly.
func Test_ConnectionIsolation(t *testing.T) {
	db, p := mustCreateDatabase()
	defer ignoreErr(db.Close)
	defer ignoreErr(func() error { return os.Remove(p) })

	r, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt("BEGIN")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}

	q, err := db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt("COMMIT")
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}

	q, err = db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

// Test_ConnectionIsolationMemory test that ISOLATION behavior of in-memory databases doesn't
// change unexpectedly.
func Test_ConnectionIsolationMemory(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer ignoreErr(db.Close)

	r, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt("BEGIN")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}

	_, err = db.QueryStringStmt("SELECT * FROM foo")
	if err == nil {
		t.Fatalf("SELECT should have received error")
	}
	if exp, got := "database is locked", err.Error(); exp != got {
		t.Fatalf("error incorrect, exp %s, got %s", exp, got)
	}
}

func Test_PartialFail(t *testing.T) {
	db, p := mustCreateDatabase()
	defer ignoreErr(db.Close)
	defer ignoreErr(func() error { return os.Remove(p) })

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	r, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"error":"UNIQUE constraint failed: foo.id"},{"last_insert_id":4,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SimpleTransaction(t *testing.T) {
	db, p := mustCreateDatabase()
	defer ignoreErr(db.Close)
	defer ignoreErr(func() error { return os.Remove(p) })

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(3, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	r, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"last_insert_id":3,"rows_affected":1},{"last_insert_id":4,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_PartialFailTransaction(t *testing.T) {
	db, p := mustCreateDatabase()
	defer ignoreErr(db.Close)
	defer ignoreErr(func() error { return os.Remove(p) })

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	r, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"error":"UNIQUE constraint failed: foo.id"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_Backup(t *testing.T) {
	db, p := mustCreateDatabase()
	defer ignoreErr(db.Close)
	defer ignoreErr(func() error { return os.Remove(p) })

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(3, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	_, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}

	dstDB := mustTempFile()
	defer ignoreErr(func() error { return os.Remove(dstDB) })

	err = db.Backup(dstDB)
	if err != nil {
		t.Fatalf("failed to backup database: %s", err.Error())
	}

	newDB, err := Open(dstDB, false)
	if err != nil {
		t.Fatalf("failed to open backup database: %s", err.Error())
	}
	defer ignoreErr(newDB.Close)
	defer ignoreErr(func() error { return os.Remove(dstDB) })
	ro, err := newDB.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_Copy(t *testing.T) {
	srcDB, p := mustCreateDatabase()
	defer ignoreErr(srcDB.Close)
	defer ignoreErr(func() error { return os.Remove(p) })

	_, err := srcDB.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(3, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	_, err = srcDB.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}

	dstFile := mustTempFile()
	defer ignoreErr(func() error { return os.Remove(dstFile) })
	dstDB, err := Open(dstFile, false)
	if err != nil {
		t.Fatalf("failed to open destination database: %s", err)
	}
	defer ignoreErr(dstDB.Close)

	err = srcDB.Copy(dstDB)
	if err != nil {
		t.Fatalf("failed to copy database: %s", err.Error())
	}

	ro, err := dstDB.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SerializeOnDisk(t *testing.T) {
	db, p := mustCreateDatabase()
	defer ignoreErr(db.Close)
	defer ignoreErr(func() error { return os.Remove(p) })

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(3, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	_, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}

	dstDB, err := os.CreateTemp("", "rqlite-bak-")
	if err != nil {
		t.Fatalf("failed to create temp file: %s", err.Error())
	}
	_ = dstDB.Close()
	defer ignoreErr(func() error { return os.Remove(dstDB.Name()) })

	// Get the bytes, and write to a temp file.
	b, err := db.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize database: %s", err.Error())
	}
	err = os.WriteFile(dstDB.Name(), b, 0644)
	if err != nil {
		t.Fatalf("failed to write serialized database to file: %s", err.Error())
	}

	newDB, err := Open(dstDB.Name(), false)
	if err != nil {
		t.Fatalf("failed to open on-disk serialized database: %s", err.Error())
	}
	defer ignoreErr(newDB.Close)
	defer ignoreErr(func() error { return os.Remove(dstDB.Name()) })
	ro, err := newDB.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SerializeInMemory(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer ignoreErr(db.Close)

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(3, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	_, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}

	dstDB, err := os.CreateTemp("", "rqlite-bak-")
	if err != nil {
		t.Fatalf("failed to create temp file: %s", err.Error())
	}
	_ = dstDB.Close()
	defer ignoreErr(func() error { return os.Remove(dstDB.Name()) })

	// Get the bytes, and write to a temp file.
	b, err := db.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize database: %s", err.Error())
	}
	err = os.WriteFile(dstDB.Name(), b, 0644)
	if err != nil {
		t.Fatalf("failed to write serialized database to file: %s", err.Error())
	}

	newDB, err := Open(dstDB.Name(), false)
	if err != nil {
		t.Fatalf("failed to open on-disk serialized database: %s", err.Error())
	}
	defer ignoreErr(newDB.Close)
	defer ignoreErr(func() error { return os.Remove(dstDB.Name()) })
	ro, err := newDB.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_Dump(t *testing.T) {
	db, p := mustCreateDatabase()
	defer ignoreErr(db.Close)
	defer ignoreErr(func() error { return os.Remove(p) })

	_, err := db.ExecuteStringStmt(chinook.DB)
	if err != nil {
		t.Fatalf("failed to load chinook dump: %s", err.Error())
	}

	var b strings.Builder
	if err := db.Dump(&b); err != nil {
		t.Fatalf("failed to dump database: %s", err.Error())
	}

	if b.String() != chinook.DB {
		t.Fatal("dumped database does not equal entered database")
	}
}

func Test_DumpMemory(t *testing.T) {
	db, p := mustCreateDatabase()
	defer ignoreErr(db.Close)
	defer ignoreErr(func() error { return os.Remove(p) })

	inmem, err := LoadIntoMemory(p, false)
	if err != nil {
		t.Fatalf("failed to create loaded in-memory database: %s", err.Error())
	}

	_, err = inmem.ExecuteStringStmt(chinook.DB)
	if err != nil {
		t.Fatalf("failed to load chinook dump: %s", err.Error())
	}

	var b strings.Builder
	if err := inmem.Dump(&b); err != nil {
		t.Fatalf("failed to dump database: %s", err.Error())
	}

	if b.String() != chinook.DB {
		t.Fatal("dumped database does not equal entered database")
	}
}

func Test_DBSTAT_table(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer ignoreErr(db.Close)

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	q, err := db.QueryStringStmt("SELECT * FROM dbstat")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `["name","path","pageno","pagetype","ncell","payload","unused","mx_payload","pgoffset","pgsize"]`, asJSON(q[0].Columns); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func Test_JSON1(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer ignoreErr(db.Close)

	_, err := db.ExecuteStringStmt("CREATE TABLE customer(name,phone)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO customer (name, phone) VALUES("fiona", json('{"mobile":"789111", "home":"123456"}'))`)
	if err != nil {
		t.Fatalf("failed to insert JSON record: %s", err.Error())
	}

	q, err := db.QueryStringStmt("SELECT customer.phone FROM customer WHERE customer.name=='fiona'")
	if err != nil {
		t.Fatalf("failed to perform simple SELECT: %s", err.Error())
	}
	if exp, got := `[{"columns":["phone"],"types":[""],"values":[["{\"mobile\":\"789111\",\"home\":\"123456\"}"]]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for simple query, expected %s, got %s", exp, got)
	}
	q, err = db.QueryStringStmt("SELECT json_extract(customer.phone, '$.mobile') FROM customer")
	if err != nil {
		t.Fatalf("failed to perform simple SELECT: %s", err.Error())
	}
	if exp, got := `[{"columns":["json_extract(customer.phone, '$.mobile')"],"types":[""],"values":[["789111"]]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for JSON query, expected %s, got %s", exp, got)
	}
	q, err = db.QueryStringStmt("SELECT customer.phone ->> '$.mobile' FROM customer")
	if err != nil {
		t.Fatalf("failed to perform simple SELECT: %s", err.Error())
	}
	if exp, got := `[{"columns":["customer.phone ->> '$.mobile'"],"types":[""],"values":[["789111"]]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for JSON query, expected %s, got %s", exp, got)
	}
}

func mustCreateDatabase() (*DB, string) {
	var err error
	f := mustTempFile()
	db, err := Open(f, false)
	if err != nil {
		panic("failed to open database")
	}

	return db, f
}

func mustCreateInMemoryDatabase() *DB {
	db, err := OpenInMemory(false)
	if err != nil {
		panic("failed to open in-memory database")
	}
	return db
}

func mustCreateInMemoryDatabaseFK() *DB {
	db, err := OpenInMemory(true)
	if err != nil {
		panic("failed to open in-memory database with foreign key constraints")
	}
	return db
}

func mustWriteAndOpenDatabase(b []byte) (*DB, string) {
	var err error
	f := mustTempFile()
	err = os.WriteFile(f, b, 0660)
	if err != nil {
		panic("failed to write file")
	}

	db, err := Open(f, false)
	if err != nil {
		panic("failed to open database")
	}
	return db, f
}

// mustExecute executes a statement, and panics on failure. Used for statements
// that should never fail, even taking into account test setup.
func mustExecute(db *DB, stmt string) {
	_, err := db.ExecuteStringStmt(stmt)
	if err != nil {
		panic(fmt.Sprintf("failed to execute statement: %s", err.Error()))
	}
}

// mustQuery executes a statement, and panics on failure. Used for statements
// that should never fail, even taking into account test setup.
func mustQuery(db *DB, stmt string) {
	_, err := db.QueryStringStmt(stmt)
	if err != nil {
		panic(fmt.Sprintf("failed to query: %s", err.Error()))
	}
}

func asJSON(v interface{}) string {
	e := encoding.Encoder{}
	b, err := e.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}

// mustTempFile returns a path to a temporary file in directory dir. It is up to the
// caller to remove the file once it is no longer needed.
func mustTempFile() string {
	tmpfile, err := os.CreateTemp("", "rqlite-db-test")
	if err != nil {
		panic(err.Error())
	}
	_ = tmpfile.Close()
	return tmpfile.Name()
}
