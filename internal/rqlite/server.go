package rqlite

import (
	"fmt"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/utc"
	"github.com/rqlite/gorqlite"
)

type serverRow struct {
	sid        string
	pid        int
	host       string
	expireAt   int64
	serverInfo string
	server     *base.ServerInfo
}

func getServer(conn *gorqlite.Connection, info *base.ServerInfo) ([]*serverRow, error) {
	return listServers(conn, fmt.Sprintf("host='%s' AND pid=%d AND sid='%s'", info.Host, info.PID, info.ServerID))
}

func listAllServers(conn *gorqlite.Connection) ([]*serverRow, error) {
	return listServers(conn, "")
}

func listServers(conn *gorqlite.Connection, where string) ([]*serverRow, error) {
	op := errors.Op("listServers")
	st := "SELECT sid, pid, host, expire_at, server_info" +
		" FROM " + ServersTable
	if len(where) > 0 {
		st += " WHERE " + where
	}

	qr, err := conn.QueryOne(st)
	if err != nil {
		return nil, NewRqliteRError(op, qr, err, st)
	}

	// no row
	if qr.NumRows() == 0 {
		return nil, nil
	}
	ret := make([]*serverRow, 0)

	for qr.Next() {
		s := &serverRow{}
		err = qr.Scan(
			&s.sid,
			&s.pid,
			&s.host,
			&s.expireAt,
			&s.serverInfo)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite scan error: %v", err))
		}
		sv, err := decodeServerInfo([]byte(s.serverInfo))
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}
		s.server = sv
		ret = append(ret, s)
	}
	return ret, nil
}

type workerRow struct {
	sid        string
	taskUuid   string
	expireAt   int64
	workerInfo string
	worker     *base.WorkerInfo
}

func listAllWorkers(conn *gorqlite.Connection) ([]*workerRow, error) {
	return listWorkers(conn, "")
}

func getWorkers(conn *gorqlite.Connection, sid string) ([]*workerRow, error) {
	return listWorkers(conn, " WHERE "+fmt.Sprintf("sid='%s'", sid))
}

func listWorkers(conn *gorqlite.Connection, where string) ([]*workerRow, error) {
	op := errors.Op("getWorkers")

	st := "SELECT sid, task_uuid, expire_at, worker_info" +
		" FROM " + WorkersTable
	if len(where) > 0 {
		st += where
	}

	qr, err := conn.QueryOne(st)
	if err != nil {
		return nil, NewRqliteRError(op, qr, err, st)
	}

	// no row
	if qr.NumRows() == 0 {
		return nil, nil
	}
	ret := make([]*workerRow, 0)

	for qr.Next() {
		s := &workerRow{}
		err = qr.Scan(
			&s.sid,
			&s.taskUuid,
			&s.expireAt,
			&s.workerInfo)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("rqlite scan error: %v", err))
		}
		sv, err := decodeWorkerInfo([]byte(s.workerInfo))
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}
		s.worker = sv
		ret = append(ret, s)
	}
	return ret, nil
}

func writeServerState(conn *gorqlite.Connection, serverInfo *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	op := errors.Op("rqlite.writeServerState")

	srvInfo, err := encodeServerInfo(serverInfo)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode server info: %v", err))
	}
	now := utc.Now()
	workersMap := map[string]string{}
	for _, w := range workers {
		wrkInfo, err := encodeWorkerInfo(w)
		if err != nil {
			continue // skip bad data
		}
		workersMap[w.ID] = wrkInfo
	}

	stmts := make([]string, 0, len(workersMap)+2)
	stmts = append(stmts, fmt.Sprintf(
		"INSERT INTO "+ServersTable+" (sid, pid, host, server_info, expire_at) "+
			" VALUES('%s', %d, '%s', '%s', %d) "+
			" ON CONFLICT(sid) DO UPDATE SET "+
			"   pid=%d,"+
			"   host='%s',"+
			"   server_info='%s',"+
			"   expire_at=%d"+
			" WHERE "+ServersTable+".expire_at<=%d",
		serverInfo.ServerID,
		serverInfo.PID,
		serverInfo.Host,
		srvInfo,
		now.Add(ttl).Unix(), //expireAt,
		serverInfo.PID,
		serverInfo.Host,
		srvInfo,
		now.Add(ttl).Unix(), //expireAt,
		now.Unix()))

	// remove expired entries
	stmts = append(stmts, fmt.Sprintf(
		"DELETE FROM "+WorkersTable+" WHERE sid='%s'"+
			" AND expire_at<=%d",
		serverInfo.ServerID,
		now.Unix()))
	for wid, wnfo := range workersMap {
		stmts = append(stmts, fmt.Sprintf("INSERT INTO "+WorkersTable+" (sid,task_uuid,expire_at,worker_info) "+
			" VALUES('%s', '%s', %d, '%s')",
			serverInfo.ServerID,
			wid,
			now.Add(ttl).Unix(), //expireAt,
			wnfo))
	}

	res, err := conn.Write(stmts)
	if err != nil {
		return NewRqliteWsError(op, res, err, stmts)
	}
	return nil
}

func clearServerState(conn *gorqlite.Connection, host string, pid int, serverID string) error {
	op := errors.Op("rqlite.clearServerState")

	stmts := make([]string, 0)
	stmts = append(stmts, fmt.Sprintf(
		"DELETE FROM "+ServersTable+" WHERE sid='%s' AND pid=%d AND host='%s'",
		serverID,
		pid,
		host))
	stmts = append(stmts, fmt.Sprintf(
		"DELETE FROM "+WorkersTable+" WHERE sid='%s'",
		serverID))

	res, err := conn.Write(stmts)
	if err != nil {
		return NewRqliteWsError(op, res, err, stmts)
	}
	return nil
}
