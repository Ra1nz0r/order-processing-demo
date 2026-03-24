package kafka

// TaskCreatedEvent описывает событие о создании задачи для публикации в Kafka.
type TaskCreatedEvent struct {
	TaskID  string `json:"task_id"` // Уникальный идентификатор задачи.
	Title   string `json:"title"`   // Краткое название задачи.
	Payload string `json:"payload"` // Произвольные данные задачи.
}
