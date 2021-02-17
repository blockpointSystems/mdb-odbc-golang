package mdb


// Result is the result of a query execution.
type Result struct {
	affectedRows int64
	insertId     int64
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *Result) LastInsertId() (int64, error) {
	return r.insertId, nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *Result) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}
