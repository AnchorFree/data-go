package line_reader

type I interface {
	//return message, offset, error
	ReadLine() ([]byte, uint64, error)
}
