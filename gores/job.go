package gores

import (
	"fmt"
	"encoding/json"

	"github.com/deckarep/golang-set"
)

// Job represents a job that needs to be executed
type Job struct {
	queue            string
	payload          map[string]string
	gores            *Gores
}

// NewJob initilizes a new job object
//func NewJob(queue string, payload map[string]interface{}, gores *Gores) *Job {
func NewJob(queue string, payload map[string]string, gores *Gores) *Job {
	return &Job{
		queue:            queue,
		payload:          payload,
		gores:            gores,
	}
}

// String returns the string representation of the job object
func (job *Job) String() string {
	res := fmt.Sprintf("Job{%s}|%s", job.queue, job.payload["name"])
	return res
}

// Payload returns the payload map inside the job struct
func (job *Job) Payload() map[string]string {
	return job.payload
}

// Retry enqueues the failed job back to Redis queue
func (job *Job) Retry(payload map[string]string) bool {
    //todo
	return true
}

// Failed update the state of the job to be failed
func (job *Job) Failed() {
	NewStat("failed", job.gores).Incr()
	NewStat(fmt.Sprintf("failed:%s", job.String()), job.gores).Incr()
}

// Processed updates the state of job to be processed
func (job *Job) Processed() {
	if job.gores == nil {
		return
	}
	NewStat("processed", job.gores).Incr()
	NewStat(fmt.Sprintf("processed:%s", job.String()), job.gores).Incr()
}

// ReserveJob uses BLPOP command to fetch job from Redis
func ReserveJob(gores *Gores, queues mapset.Set) (*Job, error) {
	queue, payload, err := gores.BlockPop(queues)
	if err != nil {
		return nil, fmt.Errorf("reserve job failed: %s", err)
	}
	return NewJob(queue, payload, gores), nil
}

// ExecuteJob executes the job, given the mapper of corresponding worker
func ExecuteJob(job *Job, tasks *map[string]interface{}) error {
	// check whether payload is valid
	jobName, ok1 := job.payload["origin"]
	jobArgs, ok2 := job.payload["data"]
	if !ok1 || !ok2 {
		return fmt.Errorf("execute job failed: job payload has no key %s or %s", "Name", "Args")
	}

    var jobArgsByte []byte = []byte(jobArgs)
	name := jobName
	var decoded map[string]interface{}
    if  err:= json.Unmarshal(jobArgsByte, &decoded); err != nil{
		return fmt.Errorf("execute job failed: job args is not a map")
    }
	args := decoded
	if !ok1 || !ok2 {
		return fmt.Errorf("execute job failed: job args is not a map or job name is not a string")
	}

	task := (*tasks)[name]
    fmt.Println(task)
	if task == nil {
		return fmt.Errorf("execute task failed: task with name %s is not registered in tasks map", jobName)
	}
	// execute targeted task
	err := task.(func(map[string]interface{}) error)(args)
	if err != nil {
		job.Failed()
		// deal with metadata here
		return fmt.Errorf("execute job failed: %s", err)
	}
	job.Processed()
	return nil
}
