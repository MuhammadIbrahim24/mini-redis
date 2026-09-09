package cli

import (
	"bufio"
	"fmt"
	"mini-redis/command"
	"mini-redis/work"
	"os"
	"strings"
)

func InteractiveCli(pool *work.Pool) {
	count := 0

	//Interactive CLI
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Ready to go")
	for scanner.Scan() {
		input := scanner.Text()
		if strings.TrimSpace(input) == "" {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(input), "EXIT") {
			break
		}

		job, err := command.Parse(input)
		if err != nil {
			fmt.Printf("Error occured while parsing the command: %v", err)
			continue
		}

		count++
		job.ID = count
		job.ResultCh = make(chan work.Result, 1)

		err = pool.Publish(job)
		if err != nil {
			fmt.Printf("Publishing job failed with error: %v", err)
			continue
		}

		result := <-job.ResultCh

		if result.Err != nil {
			value := result.Err.Error()
			fmt.Printf("%d. FAILED  %v\n", result.ID, value)

		} else {
			value := result.Value
			fmt.Printf("%d. SUCCESS %v\n", result.ID, value)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}
}
