package mdb


// Tx is a transaction.
type Tx struct {

}



func (xact *Tx) Commit() error {
	panic("implement me!")
}

func (xact *Tx) Rollback() error {
	panic("implement me!")
}
