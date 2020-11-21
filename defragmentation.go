package rose

import (
	"fmt"
	"os"
)

func createBackupDirectory() Error {
	backupDir := fmt.Sprintf("%s/backup", roseDbDir())
	if err := os.Mkdir(backupDir, os.ModePerm); err != nil {
		return &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Error trying to create backup directory for defragmentation with undelying message: %s", err.Error()),
		}
	}

	return nil
}