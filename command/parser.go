package command

import (
	"errors"
	"mini-redis/work"
	"strconv"
	"strings"
	"time"
)

var operations = map[string]int{
	"GET":    2,
	"DEL":    2,
	"EXISTS": 2,
	"COUNT":  1,
	"KEYS":   1,
}

var (
	ErrEmptyCommand         = errors.New("empty command")
	ErrInvalidOperation     = errors.New("invalid operation")
	ErrInvalidNoOfArguments = errors.New("invalid number of arguments")
)

func Parse(input string) (work.Job, error) {
	splitted := strings.Fields(input)
	if len(splitted) == 0 {
		return work.Job{}, ErrEmptyCommand
	}
	operation := strings.ToUpper(splitted[0])

	if operation != "SET" {
		arguments, ok := operations[operation]
		if !ok {
			return work.Job{}, ErrInvalidOperation
		}

		if len(splitted) != arguments {
			return work.Job{}, ErrInvalidNoOfArguments
		}
	} else if operation == "SET" && len(splitted) < 3 {
		return work.Job{}, ErrInvalidNoOfArguments
	}

	command := work.Job{Command: operation}

	if len(splitted) > 1 {
		command.Key = splitted[1]
	}
	if len(splitted) > 2 {
		if strings.ToUpper(splitted[len(splitted)-2]) == "EX" {
			seconds, err := strconv.Atoi(splitted[len(splitted)-1])
			if err == nil && seconds >= 0 {
				//not enough arguments for a valid SET command with expiry
				if len(splitted) < 5 {
					return work.Job{}, ErrInvalidNoOfArguments
				}
				command.ExpiresAt = time.Now().Add(time.Duration(seconds) * time.Second)
				command.Value = strings.Join(splitted[2:len(splitted)-2], " ")
			} else {
				command.Value = strings.Join(splitted[2:], " ")
			}
		} else {
			command.Value = strings.Join(splitted[2:], " ")
		}
	}
	return command, nil
}
