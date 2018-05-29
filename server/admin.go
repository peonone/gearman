package server

type admin struct {
}

func (a *admin) handle(txtMsg string, conn *conn) error {
	return conn.WriteTxtMsg(txtMsg + "\n")
}
