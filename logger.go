package kafka

type Logger interface {
	Debug(string)
	Info(string)
}
