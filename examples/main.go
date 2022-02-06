package main

import (
	"flag"
	"fmt"
	"log"

	"ceshi_gores/tasks"
	"github.com/meetbill/gores/gores"
)


func consume(config *gores.Config) error {
	tasks := map[string]interface{}{
		"Item":      tasks.PrintItem,
		"testjob": tasks.CalculateArea,
	}
	err := gores.Launch(config, &tasks)
	if err != nil {
		return fmt.Errorf("consume item failed: %s", err)
	}

	return nil
}

func main() {
	configPath := flag.String("c", "config.json", "path to configuration file")
	flag.Parse()

	config, err := gores.InitConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

    err = consume(config)
    if err != nil {
        log.Fatal(err)
    }
}
