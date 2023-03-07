package serializer

type Serializer interface {
	Marshal(message interface{}) ([]byte, error)
	Unmarshal(data []byte, message interface{}) error
}
