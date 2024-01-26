package logging

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gin-gonic/gin"
)

const COLOR_RED = "\033[0;31m"
const COLOR_YELLOW = "\033[0;33m"
const COLOR_GREEN = "\033[0;32m"
const COLOR_CYAN = "\033[0;36m"
const COLOR_BLUE = "\033[0;34m"
const COLOR_NONE = "\033[0m"
const COLOR_MAGENTA = "\033[0;35m"

const LEVEL_NONE = "NONE"
const LEVEL_DEBUG = "DEBUG"
const LEVEL_ERROR = "ERROR"
const LEVEL_FATAL = "FATAL"
const LEVEL_INFO = "INFO"
const LEVEL_SUCCESS = "SUCCESS"
const LEVEL_TRACE = "TRACE"
const LEVEL_WARN = "WARN"

const LEVEL_NONE_NUM = 0
const LEVEL_FATAL_NUM = 1
const LEVEL_SUCCESS_NUM = 2
const LEVEL_ERROR_NUM = 3
const LEVEL_WARN_NUM = 4
const LEVEL_INFO_NUM = 5
const LEVEL_DEBUG_NUM = 6
const LEVEL_TRACE_NUM = 7

const FORMAT_JSON = "json"
const FORMAT_CONSOLE = "console"

const METHOD_GET = COLOR_YELLOW
const METHOD_POST = COLOR_GREEN
const METHOD_PUT = COLOR_BLUE
const METHOD_PATCH = COLOR_CYAN
const METHOD_DELETE = COLOR_RED

var LogLevel = 0
var LogFormat = FORMAT_CONSOLE

var ConsoleLogFormatter = func(param gin.LogFormatterParams) string {
	if param.Latency > time.Minute {
		param.Latency = param.Latency.Truncate(time.Second)
	}

	timestamp := param.TimeStamp.Format("2006-01-02T15:04:05Z")
	statusCode := param.StatusCode
	latency := param.Latency
	clientIP := param.ClientIP
	method := param.Method
	path := param.Path
	errorMessage := param.ErrorMessage

	statusCodeColor := COLOR_GREEN
	methodColor := COLOR_MAGENTA

	level := LEVEL_INFO
	if statusCode >= 400 {
		level = LEVEL_ERROR
		statusCodeColor = COLOR_RED
	}

	switch method {
	case "GET":
		methodColor = METHOD_GET
	case "POST":
		methodColor = METHOD_POST
	case "PUT":
		methodColor = METHOD_PUT
	case "PATCH":
		methodColor = METHOD_PATCH
	case "DELETE":
		methodColor = METHOD_DELETE
	}

	return Logf(level,
		FORMAT_CONSOLE,
		timestamp,
		"%s%3d%s | %13v | %15s | %s%-7s%s %#v | %s",
		statusCodeColor,
		statusCode,
		COLOR_NONE,
		latency,
		clientIP,
		methodColor,
		method,
		COLOR_NONE,
		path,
		errorMessage,
	)
}

var JSONLogFormatter = func(param gin.LogFormatterParams) string {
	if param.Latency > time.Minute {
		param.Latency = param.Latency.Truncate(time.Second)
	}

	timestamp := param.TimeStamp.Format("2006-01-02T15:04:05Z")
	statusCode := param.StatusCode
	latency := param.Latency
	clientIP := param.ClientIP
	method := param.Method
	path := param.Path
	errorMessage := param.ErrorMessage

	statusCodeColor := COLOR_GREEN
	methodColor := COLOR_MAGENTA

	level := LEVEL_INFO
	if statusCode >= 400 {
		level = LEVEL_ERROR
		statusCodeColor = COLOR_RED
	}

	switch method {
	case "GET":
		methodColor = METHOD_GET
	case "POST":
		methodColor = METHOD_POST
	case "PUT":
		methodColor = METHOD_PUT
	case "PATCH":
		methodColor = METHOD_PATCH
	case "DELETE":
		methodColor = METHOD_DELETE
	}

	return Logf(level,
		FORMAT_CONSOLE,
		timestamp,
		"%s%3d%s | %13v | %15s | %s%-7s%s %#v | %s",
		statusCodeColor,
		statusCode,
		COLOR_NONE,
		latency,
		clientIP,
		methodColor,
		method,
		COLOR_NONE,
		path,
		errorMessage,
	)
}

func sliceIndex(list []string, val string) int {
	for i := 0; i < len(list); i++ {
		if list[i] == val {
			return i
		}
	}
	return -1
}

func SetLevel(level string) {
	levels := []string{LEVEL_NONE, LEVEL_FATAL, LEVEL_SUCCESS, LEVEL_ERROR, LEVEL_WARN, LEVEL_INFO, LEVEL_DEBUG, LEVEL_TRACE}
	levelInt := sliceIndex(levels, level)
	if levelInt == -1 {
		Fatalf("Unknown log level %s", level)
	}
	LogLevel = levelInt
}

func SetFormat(format string) {
	formats := []string{FORMAT_CONSOLE, FORMAT_JSON}
	formatInt := sliceIndex(formats, format)
	if formatInt == -1 {
		Fatalf("Unknown log level %s", format)
	}
	LogFormat = format
}

func Logf(level, formatter, timestamp, format string, args ...interface{}) string {
	if formatter == FORMAT_JSON {
		log := map[string]interface{}{
			"level":     level,
			"timestamp": timestamp,
			"message":   fmt.Sprintf(format, args...),
		}
		logBytes, _ := json.Marshal(&log)
		return string(logBytes)
	}
	switch level {
	case LEVEL_DEBUG:
		return Sdebugf(timestamp, format, args...)
	case LEVEL_ERROR:
		return Serrorf(timestamp, format, args...)
	case LEVEL_FATAL:
		return Sfatalf(timestamp, format, args...)
	case LEVEL_INFO:
		return Sinfof(timestamp, format, args...)
	case LEVEL_SUCCESS:
		return Ssuccessf(timestamp, format, args...)
	case LEVEL_TRACE:
		return Stracef(timestamp, format, args...)
	case LEVEL_WARN:
		return Swarnf(timestamp, format, args...)
	}
	return ""
}

func Debug(timestamp, message string) {
	if LogLevel >= LEVEL_DEBUG_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_DEBUG, LogFormat, timestamp, message))
		// fmt.Printf("%s[DEBUG  ]%s [%s] :: %s\n", COLOR_CYAN, COLOR_NONE, timestamp, message)
	}
}

func Debugf(timestamp, format string, args ...interface{}) {
	if LogLevel >= LEVEL_DEBUG_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_DEBUG, LogFormat, timestamp, format, args...))
		// fmt.Printf("%s[DEBUG  ]%s [%s] :: %s\n", COLOR_CYAN, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
}

func Sdebugf(timestamp, format string, args ...interface{}) string {
	if LogLevel >= LEVEL_DEBUG_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		return fmt.Sprintf("%s[DEBUG  ]%s [%s] :: %s\n", COLOR_CYAN, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
	return ""
}

func Error(timestamp, message string) {
	if LogLevel >= LEVEL_ERROR_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_ERROR, LogFormat, timestamp, message))
		// fmt.Printf("%s[ERROR  ]%s [%s] :: %s\n", COLOR_RED, COLOR_NONE, timestamp, message)
	}
}

func Errorf(timestamp, format string, args ...interface{}) {
	if LogLevel >= LEVEL_ERROR_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_ERROR, LogFormat, timestamp, format, args...))
		// fmt.Printf("%s[ERROR  ]%s [%s] :: %s\n", COLOR_RED, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
}

func Serrorf(timestamp, format string, args ...interface{}) string {
	if LogLevel >= LEVEL_ERROR_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		return fmt.Sprintf("%s[ERROR  ]%s [%s] :: %s\n", COLOR_RED, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
	return ""
}

func Fatal(timestamp, message string) {
	if LogLevel >= LEVEL_FATAL_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_FATAL, LogFormat, timestamp, message))
		// fmt.Printf("%s[FATAL  ]%s [%s] :: %s\n", COLOR_RED, COLOR_NONE, timestamp, message)
		os.Exit(1)
	}
}

func Fatalf(timestamp, format string, args ...interface{}) {
	if LogLevel >= LEVEL_FATAL_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_FATAL, LogFormat, timestamp, format, args...))
		// fmt.Printf("%s[FATAL  ]%s [%s] :: %s\n", COLOR_RED, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
		os.Exit(1)
	}
}

func Sfatalf(timestamp, format string, args ...interface{}) string {
	if LogLevel >= LEVEL_FATAL_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		return fmt.Sprintf("%s[FATAL  ]%s [%s] :: %s\n", COLOR_RED, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
	return ""
}

func Info(timestamp, message string) {
	if LogLevel >= LEVEL_INFO_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_INFO, LogFormat, timestamp, message))
		// fmt.Printf("%s[INFO   ]%s [%s] :: %s\n", COLOR_GREEN, COLOR_NONE, timestamp, message)
	}
}

func Infof(timestamp, format string, args ...interface{}) {
	if LogLevel >= LEVEL_INFO_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_INFO, LogFormat, timestamp, format, args...))
		// fmt.Printf("%s[INFO   ]%s [%s] :: %s\n", COLOR_GREEN, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
}

func Sinfof(timestamp, format string, args ...interface{}) string {
	if LogLevel >= LEVEL_INFO_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		return fmt.Sprintf("%s[INFO   ]%s [%s] :: %s\n", COLOR_GREEN, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
	return ""
}

func Success(timestamp, message string) {
	if LogLevel >= LEVEL_SUCCESS_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_SUCCESS, LogFormat, timestamp, message))
		// fmt.Printf("%s[SUCCESS]%s [%s] :: %s\n", COLOR_GREEN, COLOR_NONE, timestamp, message)
	}
}

func Successf(timestamp, format string, args ...interface{}) {
	if LogLevel >= LEVEL_SUCCESS_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_SUCCESS, LogFormat, timestamp, format, args...))
		// fmt.Printf("%s[SUCCESS]%s [%s] :: %s\n", COLOR_GREEN, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
}

func Ssuccessf(timestamp, format string, args ...interface{}) string {
	if LogLevel >= LEVEL_SUCCESS_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		return fmt.Sprintf("%s[SUCCESS]%s [%s] :: %s\n", COLOR_GREEN, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
	return ""
}

func Trace(timestamp, message string) {
	if LogLevel >= LEVEL_TRACE_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_TRACE, LogFormat, timestamp, message))
		// fmt.Printf("%s[TRACE  ]%s [%s] :: %s\n", COLOR_BLUE, COLOR_NONE, timestamp, message)
	}
}

func Tracef(timestamp, format string, args ...interface{}) {
	if LogLevel >= LEVEL_TRACE_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_TRACE, LogFormat, timestamp, format, args...))
		// fmt.Printf("%s[TRACE  ]%s [%s] :: %s\n", COLOR_BLUE, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
}

func Stracef(timestamp, format string, args ...interface{}) string {
	if LogLevel >= LEVEL_TRACE_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		return fmt.Sprintf("%s[TRACE  ]%s [%s] :: %s\n", COLOR_BLUE, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
	return ""
}

func Warn(timestamp, message string) {
	if LogLevel >= LEVEL_WARN_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_WARN, LogFormat, timestamp, message))
		// fmt.Printf("%s[WARN   ]%s [%s] :: %s\n", COLOR_YELLOW, COLOR_NONE, timestamp, message)
	}
}

func Warnf(timestamp, format string, args ...interface{}) {
	if LogLevel >= LEVEL_WARN_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Print(Logf(LEVEL_WARN, LogFormat, timestamp, format, args...))
		// fmt.Printf("%s[WARN   ]%s [%s] :: %s\n", COLOR_YELLOW, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
}

func Swarnf(timestamp, format string, args ...interface{}) string {
	if LogLevel >= LEVEL_WARN_NUM {
		if timestamp == "" {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
		return fmt.Sprintf("%s[WARN   ]%s [%s] :: %s\n", COLOR_YELLOW, COLOR_NONE, timestamp, fmt.Sprintf(format, args...))
	}
	return ""
}
