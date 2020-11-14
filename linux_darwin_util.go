// +build linux darwin

package rose

import (
	"bytes"
	"os/exec"
	"strconv"
)

/**
	Since windows seems to have a much greater limit of open file handles, it is ok
	to just return limit - 20, just in case not to reach the limit
 */
func getOpenFileHandleLimit() (int, Error) {
	cmd := exec.Command("ulimit", "-n")

	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()

	if err != nil {
		return 0, &systemError{
			Code:    SystemErrorCode,
			Message: "Could not execute ulimit -n",
		}
	}

	b := make([]uint8, 3)
	_, err = out.Read(b)

	if err != nil {
		return 0, &systemError{
			Code:    SystemErrorCode,
			Message: "Could not read from stdout after executing ulimit -n",
		}
	}

	n, err := strconv.Atoi(string(b))

	if err != nil {
		return 0, &systemError{
			Code:    SystemErrorCode,
			Message: "Could not convert given output string from ulimit -n to integer",
		}
	}

	if n > 200 {
		return 200, nil
	}

	return n - 20, nil
}


