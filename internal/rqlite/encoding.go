package rqlite

import (
	"encoding/hex"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
)

func encodeMessage(msg *base.TaskMessage) (string, error) {
	bb, err := base.EncodeMessage(msg)
	if err != nil {
		return "", errors.E(errors.Op("encodeMessage"), errors.Internal, err)
	}
	return hex.EncodeToString(bb), nil
}

func decodeMessage(data []byte) (*base.TaskMessage, error) {
	data, err := hex.DecodeString(string(data))
	if err != nil {
		return nil, errors.E(errors.Op("decodeMessage"), errors.Internal, err)
	}
	return base.DecodeMessage(data)
}

func encodeWorkerInfo(info *base.WorkerInfo) (string, error) {
	bb, err := base.EncodeWorkerInfo(info)
	if err != nil {
		return "", errors.E(errors.Op("encodeWorkerInfo"), errors.Internal, err)
	}
	return hex.EncodeToString(bb), nil
}

func decodeWorkerInfo(data []byte) (*base.WorkerInfo, error) {
	data, err := hex.DecodeString(string(data))
	if err != nil {
		return nil, errors.E(errors.Op("decodeWorkerInfo"), errors.Internal, err)
	}
	return base.DecodeWorkerInfo(data)
}

func encodeServerInfo(info *base.ServerInfo) (string, error) {
	bb, err := base.EncodeServerInfo(info)
	if err != nil {
		return "", errors.E(errors.Op("encodeServerInfo"), errors.Internal, err)
	}
	return hex.EncodeToString(bb), nil
}

func decodeServerInfo(data []byte) (*base.ServerInfo, error) {
	data, err := hex.DecodeString(string(data))
	if err != nil {
		return nil, errors.E(errors.Op("decodeServerInfo"), errors.Internal, err)
	}
	return base.DecodeServerInfo(data)
}

func encodeSchedulerEnqueueEvent(ev *base.SchedulerEnqueueEvent) (string, error) {
	bb, err := base.EncodeSchedulerEnqueueEvent(ev)
	if err != nil {
		return "", errors.E(errors.Op("encodeSchedulerEnqueueEvent"), errors.Internal, err)
	}
	return hex.EncodeToString(bb), nil
}

func decodeSchedulerEnqueueEvent(data []byte) (*base.SchedulerEnqueueEvent, error) {
	data, err := hex.DecodeString(string(data))
	if err != nil {
		return nil, errors.E(errors.Op("decodeSchedulerEnqueueEvent"), errors.Internal, err)
	}
	return base.DecodeSchedulerEnqueueEvent(data)
}

func encodeSchedulerEntry(entry *base.SchedulerEntry) (string, error) {
	bb, err := base.EncodeSchedulerEntry(entry)
	if err != nil {
		return "", errors.E(errors.Op("encodeSchedulerEntry"), errors.Internal, err)
	}
	return hex.EncodeToString(bb), nil
}

func decodeSchedulerEntry(data []byte) (*base.SchedulerEntry, error) {
	data, err := hex.DecodeString(string(data))
	if err != nil {
		return nil, errors.E(errors.Op("decodeSchedulerEntry"), errors.Internal, err)
	}
	return base.DecodeSchedulerEntry(data)
}
