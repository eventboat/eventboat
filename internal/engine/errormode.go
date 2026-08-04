package engine

import (
	"strings"

	"github.com/edgesets/edgestream/internal/message"
)

type errorMode string

const (
	errModePropagate errorMode = "propagate"
	errModeIgnore    errorMode = "ignore"
	errModeSilent    errorMode = "silent"
)

func parseErrorMode(raw string) errorMode {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "ignore":
		return errModeIgnore
	case "silent":
		return errModeSilent
	default:
		return errModePropagate
	}
}

func (p *Pipeline) errorModeForStage(stageID string) errorMode {
	if stageID != "" {
		if mode, ok := p.stageErrorMode[stageID]; ok && mode != "" {
			return parseErrorMode(mode)
		}
	}
	return parseErrorMode(p.ir.Engine.ErrorMode)
}

// handleEvalError applies error_mode for DSL evaluation failures.
// Returns (treatAsFalse, ackError). When treatAsFalse is true, callers should
// treat the condition as non-matching and ack with nil unless ackError is set.
func (p *Pipeline) handleEvalError(stageID string, err error) (treatAsFalse bool, ackError error) {
	if err == nil {
		return false, nil
	}
	switch p.errorModeForStage(stageID) {
	case errModeIgnore:
		if p.metrics != nil {
			p.metrics.IncStageError(p.ir.Name, stageID, "dsl_ignore")
		}
		return true, nil
	case errModeSilent:
		return true, nil
	default:
		return false, err
	}
}

func (p *Pipeline) ackMessageError(stageID string, msg *message.Message, err error) {
	treatAsFalse, ackErr := p.handleEvalError(stageID, err)
	if treatAsFalse {
		msg.Ack(nil)
		return
	}
	msg.Ack(ackErr)
}

func (p *Pipeline) ackBatchError(stageID string, batch []*message.Message, err error) {
	treatAsFalse, ackErr := p.handleEvalError(stageID, err)
	for _, m := range batch {
		if treatAsFalse {
			m.Ack(nil)
		} else {
			m.Ack(ackErr)
		}
	}
}
