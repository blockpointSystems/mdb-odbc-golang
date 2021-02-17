package mdb

func (db *Conn) GetMaxPacketSize() int {
	if db != nil {
		return db.cfg.MaxAllowedPacket
	}
	return 0
}