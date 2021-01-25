// +build linux darwin

package rose

import (
	"bytes"
	"os/exec"
	"strconv"
	"strings"
)

/**
	Since windows seems to have a much greater limit of open file handles, it is ok
	to just return limit - 20, just in case not to reach the limit
 */
func getOpenFileHandleLimit() (int, Error) {
	return 200, nil
/*	cmd := exec.Command("ulimit", "-n")

	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()

	if err != nil {
		return 0, newError(SystemMasterErrorCode, OperatingSystemCode, "Could not execute ulimit -n")
	}

	b := make([]uint8, 3)
	_, err = out.Read(b)

	if err != nil {
		return 0, newError(SystemMasterErrorCode, OperatingSystemCode, "Could not read from stdout after executing ulimit -n")
	}

	n, err := strconv.Atoi(string(b))

	if err != nil {
		return 0, newError(SystemMasterErrorCode, DataConversionCode, "Could not convert given output string from ulimit -n to integer")
	}

	if n > 200 {
		return 200, nil
	}

	return n - 20, nil*/
}

func getDbSize() (int, Error) {
	cmd := exec.Command("du", roseDbDir())

	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()

	if err != nil {
		return 0, newError(SystemMasterErrorCode, OperatingSystemCode, "Could not execute du {dir}")
	}

	b := make([]uint8, 1000)
	_, err = out.Read(b)

	if err != nil {
		return 0, newError(SystemMasterErrorCode, OperatingSystemCode, "Could not read from stdout after executing du {dir}")
	}

	split := strings.Split(string(b), "\t")
	n, err := strconv.Atoi(split[0])

	if err != nil {
		return 0, newError(SystemMasterErrorCode, DataConversionCode, "Could not convert given output string from ulimit -n to integer")
	}

	return n, nil
}

func getDiskSize() (int, Error) {
	cmd := exec.Command("du", roseDbDir())

	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()

	if err != nil {
		return 0, newError(SystemMasterErrorCode, OperatingSystemCode, "Could not execute du {dir}")
	}

	b := make([]uint8, 1000)
	_, err = out.Read(b)

	if err != nil {
		return 0, newError(SystemMasterErrorCode, OperatingSystemCode, "Could not read from stdout after executing du {dir}")
	}

	split := strings.Split(string(b), "\t")
	n, err := strconv.Atoi(split[0])

	if err != nil {
		return 0, newError(SystemMasterErrorCode, DataConversionCode, "Could not convert given output string from ulimit -n to integer")
	}

	return n, nil
}


