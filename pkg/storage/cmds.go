package storage

import (
	"fmt"
	"regexp"
	"time"
)

func GetBackupDirSuffix() string {
	return fmt.Sprintf("_old_%d", time.Now().Unix())
}

func getBackDir(origin string) string {
	return origin + GetBackupDirSuffix()
}

func mvDirCommand(from string, to string) string {
	if from != "" && to != "" {
		return fmt.Sprintf("mv %s %s", from, to)
	}
	return ""
}

var invalidDstRegex = `(^/+$)|(\s+)`
var allowedDstRegex = `\S+_old_[0-9]+$`

func sanityCheckForRM(dst string) bool {
	invalid, _ := regexp.Compile(invalidDstRegex)
	if invalid.MatchString(dst) {
		return false
	}
	allowed, _ := regexp.Compile(allowedDstRegex)
	if !allowed.MatchString(dst) {
		return false
	}
	return true
}

func rmDirCommand(dst string) string {
	if dst != "" && sanityCheckForRM(dst) {
		return fmt.Sprintf("rm -r %s 2>/dev/null", dst)
	}
	return ""
}

func mkDirCommand(dst string) string {
	if dst != "" {
		return fmt.Sprintf("mkdir -p %s", dst)
	}
	return ""
}

func mvAndMkDirCommand(srcDir string, bkDir string) string {
	mvCmd := mvDirCommand(srcDir, bkDir)
	mkCmd := mkDirCommand(srcDir)
	return fmt.Sprintf("%s && %s", mvCmd, mkCmd)
}
