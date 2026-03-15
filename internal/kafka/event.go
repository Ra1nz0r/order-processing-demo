package kafka

type TaskCreatedEvent struct {
	TaskID  string `json:"task_id"`
	Title   string `json:"title"`
	Payload string `json:"payload"`
}
