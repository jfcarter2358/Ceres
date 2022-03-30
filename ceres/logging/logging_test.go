package logging

import (
	"bytes"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
)

func captureOutput(f func()) string {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	f()
	log.SetOutput(os.Stderr)
	return buf.String()
}

func TestInitialize(t *testing.T) {
	Initialize("trace")
	if log.GetLevel() != log.TraceLevel {
		t.Errorf("Log level was incorrect, got: %s, want: %s", log.GetLevel(), log.TraceLevel)
	}
	Initialize("debug")
	if log.GetLevel() != log.DebugLevel {
		t.Errorf("Log level was incorrect, got: %s, want: %s", log.GetLevel(), log.DebugLevel)
	}
	Initialize("info")
	if log.GetLevel() != log.InfoLevel {
		t.Errorf("Log level was incorrect, got: %s, want: %s", log.GetLevel(), log.InfoLevel)
	}
	Initialize("warn")
	if log.GetLevel() != log.WarnLevel {
		t.Errorf("Log level was incorrect, got: %s, want: %s", log.GetLevel(), log.WarnLevel)
	}
	Initialize("error")
	if log.GetLevel() != log.ErrorLevel {
		t.Errorf("Log level was incorrect, got: %s, want: %s", log.GetLevel(), log.ErrorLevel)
	}
	Initialize("fatal")
	if log.GetLevel() != log.FatalLevel {
		t.Errorf("Log level was incorrect, got: %s, want: %s", log.GetLevel(), log.FatalLevel)
	}
	Initialize("panic")
	if log.GetLevel() != log.PanicLevel {
		t.Errorf("Log level was incorrect, got: %s, want: %s", log.GetLevel(), log.PanicLevel)
	}
}

func TestInitializePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic with an invalid log level")
		}
	}()

	Initialize("foobar")
}

func TestTRACE(t *testing.T) {
	expectedOutput := "{\"level\":\"trace\",\"msg\":\"This is a test\",\"time\":"

	Initialize("trace")
	output := captureOutput(func() {
		TRACE("This is a test")
	})
	output = output[:len(output)-29]
	if output != expectedOutput {
		t.Errorf("Log message was incorrect, got: %s, want: %s", output, expectedOutput)
	}
}

func TestDEBUG(t *testing.T) {
	expectedOutput := "{\"level\":\"debug\",\"msg\":\"This is a test\",\"time\":"

	Initialize("debug")
	output := captureOutput(func() {
		DEBUG("This is a test")
	})
	output = output[:len(output)-29]
	if output != expectedOutput {
		t.Errorf("Log message was incorrect, got: %s, want: %s", output, expectedOutput)
	}
}

func TestINFO(t *testing.T) {
	expectedOutput := "{\"level\":\"info\",\"msg\":\"This is a test\",\"time\":"

	Initialize("info")
	output := captureOutput(func() {
		INFO("This is a test")
	})
	output = output[:len(output)-29]
	if output != expectedOutput {
		t.Errorf("Log message was incorrect, got: %s, want: %s", output, expectedOutput)
	}
}

func TestWARN(t *testing.T) {
	expectedOutput := "{\"level\":\"warning\",\"msg\":\"This is a test\",\"time\":"

	Initialize("warn")
	output := captureOutput(func() {
		WARN("This is a test")
	})
	output = output[:len(output)-29]
	if output != expectedOutput {
		t.Errorf("Log message was incorrect, got: %s, want: %s", output, expectedOutput)
	}
}

func TestERROR(t *testing.T) {
	expectedOutput := "{\"level\":\"error\",\"msg\":\"This is a test\",\"time\":"

	Initialize("error")
	output := captureOutput(func() {
		ERROR("This is a test")
	})
	output = output[:len(output)-29]
	if output != expectedOutput {
		t.Errorf("Log message was incorrect, got: %s, want: %s", output, expectedOutput)
	}
}

func TestFATAL(t *testing.T) {
	defer func() { log.StandardLogger().ExitFunc = nil }()
	var fatal bool
	log.StandardLogger().ExitFunc = func(int) { fatal = true }
	expectedOutput := "{\"level\":\"fatal\",\"msg\":\"This is a test\",\"time\":"

	Initialize("fatal")
	fatal = false
	output := captureOutput(func() {
		FATAL("This is a test")
	})
	output = output[:len(output)-29]
	if output != expectedOutput || fatal == false {
		t.Errorf("Log message was incorrect, got: %s, want: %s", output, expectedOutput)
	}
}

func TestPANIC(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	expectedOutput := "{\"level\":\"panic\",\"msg\":\"This is a test\",\"time\":"

	Initialize("panic")
	output := captureOutput(func() {
		PANIC("This is a test")
	})
	output = output[:len(output)-29]
	if output != expectedOutput {
		t.Errorf("Log message was incorrect, got: %s, want: %s", output, expectedOutput)
	}
}
