package bdb

/*
 #cgo LDFLAGS: -ldb
 #include <stdio.h>
 #include <db.h>
 #include <stdlib.h>
 #include <errno.h>
 static inline int db_env_open(DB_ENV *env, const char *home, u_int32_t flags, int mode) {
 	return env->open(env, home, flags, mode);
 }
 static inline int db_env_set_cachesize(DB_ENV *env, u_int32_t gbytes, u_int32_t bytes, int ncache) {
 	return env->set_cachesize(env, gbytes, bytes, ncache);
 }
 static inline int db_env_set_flags(DB_ENV *env, u_int32_t flags, int onoff) {
 	return env->set_flags(env, flags, onoff);
 }
 static inline int db_env_set_log_config(DB_ENV *env, u_int32_t flags, int onoff) {
 	return env->log_set_config(env, flags, onoff);
 }
 static inline int db_env_set_lk_max_lockers(DB_ENV *env, u_int32_t max) {
 	return env->set_lk_max_lockers(env, max);
 }
 static inline int db_env_set_lk_max_locks(DB_ENV *env, u_int32_t max) {
 	return env->set_lk_max_locks(env, max);
 }
 static inline int db_env_set_lk_max_objects(DB_ENV *env, u_int32_t max) {
 	return env->set_lk_max_objects(env, max);
 }
 static inline int db_env_set_tx_max(DB_ENV *env, u_int32_t max) {
 	return env->set_tx_max(env, max);
 }
 static inline int db_lock_detect(DB_ENV *env, u_int32_t flags, u_int32_t atype) {
 	return env->lock_detect(env, flags, atype, NULL);
 }
 static inline int db_memp_trickle(DB_ENV *env, int pct, int *nwrotep) {
 	return env->memp_trickle(env, pct, nwrotep);
 }
 static inline int db_env_set_lg_bsize(DB_ENV *env, u_int32_t lg_bsize) {
 	return env->set_lg_bsize(env, lg_bsize);
 }
 static inline int db_env_txn_begin(DB_ENV *env, DB_TXN **txn, u_int32_t flags) {
 	return env->txn_begin(env, NULL, txn, flags);
 }
 static inline int db_env_dbremove(DB_ENV *env, DB_TXN *txn, const char *file, const char *database, u_int32_t flags) {
 	return env->dbremove(env, txn, file, database, flags);
 }
 static inline int db_env_close(DB_ENV *env, u_int32_t flags) {
 	return env->close(env, flags);
 }
 static inline int db_open(DB *db, DB_TXN *txn, const char *file, const char *database, DBTYPE type, u_int32_t flags, int mode) {
 	return db->open(db, txn, file, database, type, flags, mode);
 }
 static inline int db_put(DB *db, DB_TXN *txn, DBT *key, DBT *data, u_int32_t flags) {
 	return db->put(db, txn, key, data, flags);
 }
 static inline int db_get(DB *db, DB_TXN *txn, DBT *key, DBT *data, u_int32_t flags) {
 	return db->get(db, txn, key, data, flags);
 }
 static inline int db_close(DB *db, u_int32_t flags) {
 	return db->close(db, flags);
 }
 static inline int db_del(DB *db, DB_TXN *txn, DBT *key, u_int32_t flags) {
 	return db->del(db, txn, key, flags);
 }
 static inline int db_txn_abort(DB_TXN *txn) {
 	return txn->abort(txn);
 }
 static inline int db_txn_commit(DB_TXN *txn, u_int32_t flags) {
 	return txn->commit(txn, flags);
 }
 static inline int db_cursor(DB *db, DB_TXN *txn, DBC **cursor, u_int32_t flags) {
 	return db->cursor(db, txn, cursor, flags);
 }
 static inline int db_cursor_get(DBC *cur, DBT *key, DBT *data, u_int32_t flags) {
 	return cur->get(cur, key, data, flags);
 }
 static inline int db_cursor_close(DBC *cur) {
 	return cur->close(cur);
 }
 static inline int db_set_q_extentsize(DB *db, u_int32_t size) {
 	return db->set_q_extentsize(db, size);;
 }
 static inline int db_set_re_len(DB *db, u_int32_t len) {
 	return db->set_re_len(db, len);;
 }
 static inline int db_set_pagesize(DB *db, u_int32_t size) {
 	return db->set_pagesize(db, size);;
 }
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"fmt"
	l4g "github.com/alecthomas/log4go"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var (
	globalEnv                  Env
	globalEnvConfig            EnvConfig
	globalQListDb              Db
	globalQCursorListDb        Db
	globalQlistMap             map[string]*Queue
	globalQlistMapRWLock       = sync.RWMutex{}
	globalCursorQlistMap       map[string]*Cursor
	globalQCursorListMapRWLock = sync.RWMutex{}
	globalDaemonQuitChan       chan bool
)

func InitBdbEnv(envConfig EnvConfig) {
	globalEnvConfig = envConfig

	globalQlistMap = make(map[string]*Queue)
	globalCursorQlistMap = make(map[string]*Cursor)

	var err error
	err = OpenDbEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db env err:%s", err.Error())
		os.Exit(-1)
	}

	dbQCursorConfig := &DbConfig{
		file:     globalEnvConfig.Home + "/queue.cursor",
		fileMode: 0664,
	}

	err = OpenQcursorDb(dbQCursorConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db env err:%s", err.Error())
		os.Exit(-1)
	}

	dbQListConfig := &DbConfig{
		file:     globalEnvConfig.Home + "/queue.list",
		fileMode: 0664,
	}

	err = OpenQlistDb(dbQListConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db env err:%s", err.Error())
		os.Exit(-1)
	}

	go QstatsDumpTick()
	go CheckMempoolTrickleTick()
	go CheckPointTick()
	go CheckDeadLockDetectTick()
}

type EnvConfig struct {
	Home                  string
	CacheSize             int32
	TxnNoSync             bool
	TxMax                 int32
	TxLogBufferSize       int32
	AutoRemoveLog         bool
	MaxLock               int32
	PageSize              int32
	DeadlockDetectVal     int
	CheckpointVal         int
	MempoolTrickleVal     int
	MempoolTricklePercent int
	QstatsDumpVal         int
	ReLen                 int32
	QExtentsize           int32
}

type Env struct {
	ptr *C.DB_ENV
}

type DbErrNo int

func (err DbErrNo) Error() string {
	return C.GoString(C.db_strerror(C.int(err)))
}

//define some errors
const (
	DB_ERR_AGAIN           = DbErrNo(C.EAGAIN)
	DB_ERR_INVALID         = DbErrNo(C.EINVAL)
	DB_ERR_NOENTRY         = DbErrNo(C.ENOENT)
	DB_ERR_EXISTS          = DbErrNo(C.EEXIST)
	DB_ERR_ACCESS          = DbErrNo(C.EACCES)
	DB_ERR_NOSPACE         = DbErrNo(C.ENOSPC)
	DB_ERR_PERMISSION      = DbErrNo(C.EPERM)
	DB_ERR_RUNRECOVERY     = DbErrNo(C.DB_RUNRECOVERY)
	DB_ERR_VERSIONMISMATCH = DbErrNo(C.DB_VERSION_MISMATCH)
	DB_ERR_OLDVERSION      = DbErrNo(C.DB_OLD_VERSION)
	DB_ERR_LOCKDEADLOCK    = DbErrNo(C.DB_LOCK_DEADLOCK)
	DB_ERR_LOCKNOTGRANTED  = DbErrNo(C.DB_LOCK_NOTGRANTED)
	DB_ERR_BUFFERTOOSMALL  = DbErrNo(C.DB_BUFFER_SMALL)
	DB_ERR_SECONDARYBAD    = DbErrNo(C.DB_SECONDARY_BAD)
	DR_ERR_FOREIGNCONFLICT = DbErrNo(C.DB_FOREIGN_CONFLICT)
	DB_ERR_KEYEXISTS       = DbErrNo(C.DB_KEYEXIST)
	DB_ERR_KEYEMPTY        = DbErrNo(C.DB_KEYEMPTY)
	DB_ERR_NOTFOUND        = DbErrNo(C.DB_NOTFOUND)
)

const (
	QUEUE_NAME_DELIMITER = "#"
	MAX_DBP_PER_QUEUE    = 128
	MAX_QUEUE_NAME_SIZE  = 256
	MAX_ITEM_PER_DBP     = 1000000
)

func OpenDbEnv() (err error) {
	var ret C.int
	ret = C.db_env_create(&globalEnv.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	defer func() {
		if ret != 0 && globalEnv.ptr != nil {
			C.db_env_close(globalEnv.ptr, 0)
		}
	}()

	/* set MPOOL size */
	var cacheSize C.u_int32_t = C.u_int32_t(globalEnvConfig.CacheSize)
	ret = C.db_env_set_cachesize(globalEnv.ptr, 0, cacheSize, 0)

	/* set DB_TXN_NOSYNC flag */
	if globalEnvConfig.TxnNoSync {
		C.db_env_set_flags(globalEnv.ptr, C.DB_TXN_NOSYNC, 1)
	}

	/* auto remove log */
	if globalEnvConfig.AutoRemoveLog {
		C.db_env_set_log_config(globalEnv.ptr, C.DB_LOG_AUTO_REMOVE, 1)
	}

	/* set locking */
	var maxLocks C.u_int32_t = C.u_int32_t(globalEnvConfig.MaxLock)
	C.db_env_set_lk_max_lockers(globalEnv.ptr, maxLocks)
	C.db_env_set_lk_max_lockers(globalEnv.ptr, maxLocks)
	C.db_env_set_lk_max_objects(globalEnv.ptr, maxLocks)

	/* at least max active transactions */
	var txMax C.u_int32_t = C.u_int32_t(globalEnvConfig.TxMax)
	C.db_env_set_tx_max(globalEnv.ptr, txMax)

	/* set transaction log buffer */
	var txLogBufferSize C.u_int32_t = C.u_int32_t(globalEnvConfig.TxLogBufferSize)
	C.db_env_set_lg_bsize(globalEnv.ptr, txLogBufferSize)

	if _, err = os.Stat(globalEnvConfig.Home); os.IsNotExist(err) {
		err = os.Mkdir(globalEnvConfig.Home, 0750)
		if err != nil {
			return
		}
	}

	var flags C.u_int32_t = C.DB_CREATE | C.DB_INIT_LOCK /*| C.DB_THREAD */ | C.DB_INIT_MPOOL | C.DB_INIT_LOG | C.DB_INIT_TXN | C.DB_RECOVER
	var mode C.int = 0
	var home *C.char = C.CString(globalEnvConfig.Home)
	defer C.free(unsafe.Pointer(home))
	ret = C.db_env_open(globalEnv.ptr, home, flags, mode)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	return
}

func CloseDbEnv() (err error) {
	var ret C.int
	ret = C.db_env_close(globalEnv.ptr, C.u_int32_t(C.DB_FORCESYNC))
	if ret != 0 {
		return DbErrNo(ret)
	}
	return
}

type DbType int

const (
	DBBTree    = DbType(C.DB_BTREE)
	DBHash     = DbType(C.DB_HASH)
	DBNumbered = DbType(C.DB_RECNO)
	DBQueue    = DbType(C.DB_QUEUE)
	DBUnknown  = DbType(C.DB_UNKNOWN)
)

type DbConfig struct {
	file     string
	fileMode os.FileMode
	create   bool
}

type Db struct {
	ptr *C.DB
}

type Transaction struct {
	ptr *C.DB_TXN
}

// Database cursor.
type DbCursor struct {
	db  Db
	ptr *C.DBC
}

func OpenQcursorDb(dbConfig *DbConfig) (err error) {
	l4g.Debug("start to OpenQcursorDb")
	if dbConfig == nil {
		err = errors.New("dbConfig is nil")
		return
	}

	var ret C.int
	var txn = Transaction{}
	ret = C.db_create(&globalQCursorListDb.ptr, globalEnv.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	defer func() {
		if ret != 0 && globalQCursorListDb.ptr != nil {
			C.db_close(globalQCursorListDb.ptr, 0)
			globalQCursorListDb.ptr = nil
		}
	}()

	ret = C.db_env_txn_begin(globalEnv.ptr, &txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	var flags C.u_int32_t = C.DB_CREATE
	var file *C.char = C.CString(dbConfig.file)
	defer func() {
		C.free(unsafe.Pointer(file))
		if err != nil {
			C.db_txn_abort(txn.ptr)
		}

	}()

	ret = C.db_open(globalQCursorListDb.ptr, txn.ptr, file, nil, C.DB_BTREE, flags, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	dbCursor := DbCursor{
		db: globalQCursorListDb,
	}

	ret = C.db_cursor(globalQCursorListDb.ptr, txn.ptr, &dbCursor.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	defer func() {
		if err != nil {
			C.db_cursor_close(dbCursor.ptr)
		}

	}()

	var dbKey, dbData C.DBT
	for {
		ret = C.db_cursor_get(dbCursor.ptr, &dbKey, &dbData, C.DB_NEXT)
		if ret != 0 {
			break
		}
		//

		key := C.GoBytes(dbKey.data, C.int(dbKey.size))
		value := C.GoBytes(dbData.data, C.int(dbData.size))

		cur := binary.BigEndian.Uint32(value[0:4])
		min := binary.BigEndian.Uint32(value[4:8])
		max := binary.BigEndian.Uint32(value[8:])

		cursor := &Cursor{
			cur: uint32(cur),
			min: uint32(min),
			max: uint32(max),
		}
		globalCursorQlistMap[string(key)] = cursor
		l4g.Debug("open cursor name:%s,cur:%d,min:%d,max:%d", string(key), cur, min, max)
	}

	if ret != C.int(DB_ERR_NOTFOUND) {
		err = DbErrNo(ret)
		return
	}

	ret = C.db_cursor_close(dbCursor.ptr)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	ret = C.db_txn_commit(txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	l4g.Debug("end to OpenQcursorDb")
	return
}

func OpenQlistDb(dbConfig *DbConfig) (err error) {
	l4g.Debug("start to OpenQlistDb")
	if dbConfig == nil {
		err = errors.New("dbConfig is nil")
		return
	}

	var ret C.int
	var txn = Transaction{}
	ret = C.db_create(&globalQListDb.ptr, globalEnv.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	defer func() {
		if ret != 0 && globalQListDb.ptr != nil {
			C.db_close(globalQListDb.ptr, 0)
			globalQListDb.ptr = nil
		}
	}()

	ret = C.db_env_txn_begin(globalEnv.ptr, &txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	var flags C.u_int32_t = C.DB_CREATE
	var file *C.char = C.CString(dbConfig.file)
	defer func() {
		C.free(unsafe.Pointer(file))
		if err != nil {
			C.db_txn_abort(txn.ptr)
		}

	}()

	ret = C.db_open(globalQListDb.ptr, txn.ptr, file, nil, C.DB_BTREE, flags, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	dbCursor := DbCursor{
		db: globalQListDb,
	}

	ret = C.db_cursor(globalQListDb.ptr, txn.ptr, &dbCursor.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	defer func() {
		if err != nil {
			C.db_cursor_close(dbCursor.ptr)
		}

	}()

	var dbKey, dbData C.DBT
	var queueName []byte
	var stats []byte

	for {
		ret = C.db_cursor_get(dbCursor.ptr, &dbKey, &dbData, C.DB_NEXT)
		if ret != 0 {
			break
		}
		//
		queueName = C.GoBytes(dbKey.data, C.int(dbKey.size))
		stats = C.GoBytes(dbData.data, C.int(dbData.size))
		var setHit = binary.BigEndian.Uint32(stats[0:4])
		var getHit = binary.BigEndian.Uint32(stats[4:])
		l4g.Debug("open queue name:%s,get:%d,set:%d", string(queueName), getHit, setHit)
		qstats := &QStats{
			getHits: uint32(getHit),
			setHits: uint32(setHit),
		}
		openExistedQueue(txn, string(queueName), qstats)
	}

	if ret != C.int(DB_ERR_NOTFOUND) {
		err = DbErrNo(ret)
		return
	}

	ret = C.db_cursor_close(dbCursor.ptr)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	ret = C.db_txn_commit(txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	l4g.Debug("end to OpenQlistDb")
	return
}

func (db Db) Close() (err error) {
	var ret C.int
	ret = C.db_close(db.ptr, 0)
	if ret != 0 {
		return DbErrNo(ret)
	}
	return
}

type Queue struct {
	maxDbpPerQueue   int32
	maxQueueNameSize int32
	dbs              map[uint32]Db
	dbNames          map[uint32]string
	setHits          uint32
	getHits          uint32
	oldSetHits       uint32
	oldGetHits       uint32
	mutex            sync.Mutex
}

type QStats struct {
	setHits uint32
	getHits uint32
}

type Cursor struct {
	cur   uint32
	max   uint32
	min   uint32
	db    Db
	mutex sync.Mutex
}

func openExistedQueue(txn Transaction, key string, qstats *QStats) (err error) {

	var queueName string
	if strings.Contains(key, QUEUE_NAME_DELIMITER) {
		strs := strings.Split(key, QUEUE_NAME_DELIMITER)
		if len(strs) != 2 {
			err = errors.New("invalid key")
			return
		}

		queueName = strs[0]
	} else {
		queueName = key
	}

	queue := &Queue{
		setHits:    qstats.setHits,
		oldSetHits: qstats.setHits,
		getHits:    qstats.getHits,
		oldGetHits: qstats.getHits,
		dbs:        make(map[uint32]Db),
		dbNames:    make(map[uint32]string),
	}

	queue.mutex.Lock()

	defer func() {
		queue.mutex.Unlock()
	}()
	maxDbId := (queue.setHits) / MAX_ITEM_PER_DBP
	if queue.setHits != 0 && queue.setHits%MAX_ITEM_PER_DBP == 0 {
		maxDbId = maxDbId - 1
	}

	var ret C.int
	var tmp string
	var tmpqn *C.char
	defer func() {
		C.free(unsafe.Pointer(tmpqn))
	}()

	var i uint32
	for i = 0; i <= maxDbId; i++ {
		db := Db{}
		ret = C.db_create(&db.ptr, globalEnv.ptr, 0)
		if ret != 0 {
			err = DbErrNo(ret)
			l4g.Error("openExistedQueue err:%s", err)
			return
		}

		tmp = fmt.Sprintf("%s_%d", queueName, i)
		tmpqn = C.CString(tmp)
		ret = C.db_open(db.ptr, txn.ptr, tmpqn, nil, C.DB_RECNO, C.DB_CREATE, 0664)
		if ret != 0 {
			err = DbErrNo(ret)
			l4g.Error("openExistedQueue err:%s", err)
			return
		}
		queue.dbs[i] = db
		queue.dbNames[i] = tmp

		if i == maxDbId {
			var position uint32
			position, err = db.getLastRecord(txn)
			if err != nil {
				l4g.Error("getLastRecord err:%s", err)
				return
			}

			var newHits = uint32(position) + MAX_ITEM_PER_DBP*maxDbId
			l4g.Debug("db[key:%s][old_set_hits:%d][maxdbpid:%d][repaired_recno:%d][new_id:%d]", key, queue.setHits, maxDbId, position, newHits)
			queue.setHits = newHits
		}

	}

	globalQlistMap[queueName] = queue
	return
}

func (db Db) getLastRecord(txn Transaction) (position uint32, err error) {
	var ret C.int
	var dbKey, dbData C.DBT
	dbKey.size = 4

	dbCursor := DbCursor{
		db: db,
	}

	ret = C.db_cursor(db.ptr, txn.ptr, &dbCursor.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	ret = C.db_cursor_get(dbCursor.ptr, &dbKey, &dbData, C.DB_LAST)
	if ret < 0 {
		if ret != C.DB_NOTFOUND {
			err = errors.New("db not found")
			return
		}
	}

	var keyByte = C.GoBytes(dbKey.data, C.int(dbKey.size))
	position = binary.LittleEndian.Uint32(keyByte[0:4])
	l4g.Debug("last position=%d", position)
	return
}

func CloseQlistDb() (err error) {
	var ret C.int

	err = DumpQstatsToDb()
	if err != nil {
		l4g.Error("dumpQstatsToDb err:%s", err)
		return
	}

	for _, q := range globalQlistMap {
		if q != nil {
			for kk, db := range q.dbs {
				ret = C.db_close(db.ptr, 0)
				if ret != 0 {
					l4g.Error("db close err:%s,kk:%d", err, kk)
					err = DbErrNo(ret)
					return
				}
				delete(q.dbs, kk)
			}
		}
	}

	err = globalQListDb.Close()
	if err != nil {
		l4g.Error("globalQListDb close err:%s", err)
		return
	}

	err = globalQCursorListDb.Close()
	if err != nil {
		l4g.Error("globalQCursorListDb close err:%s", err)
		return
	}

	return
}

func (queue *Queue) NewQueueAndInsert(key string) (err error) {
	var ret C.int
	var queueName string

	if strings.Contains(key, QUEUE_NAME_DELIMITER) {
		strs := strings.Split(key, QUEUE_NAME_DELIMITER)
		if len(strs) != 2 {
			err = errors.New("invalid key")
			return
		}

		queueName = strs[0]
	} else {
		queueName = key
	}

	globalQlistMapRWLock.Lock()
	defer func() {
		globalQlistMapRWLock.Unlock()
	}()

	var txn Transaction
	ret = C.db_env_txn_begin(globalEnv.ptr, &txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	defer func() {
		if err != nil {
			C.db_txn_abort(txn.ptr)
		}
	}()

	err = queue.NewDBInQueue(txn, queueName, 0)
	if err != nil {
		return
	}

	var dbKey, dbData C.DBT
	var keyByte = []byte(queueName)
	var valueByte = make([]byte, 8)
	dbKey.data = unsafe.Pointer(&keyByte[0])
	dbKey.size = C.u_int32_t(len(keyByte))
	dbData.data = unsafe.Pointer(&valueByte[0])
	dbData.size = 8

	ret = C.db_put(globalQListDb.ptr, txn.ptr, &dbKey, &dbData, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	ret = C.db_txn_commit(txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	globalQlistMap[queueName] = queue
	return
}

func (queue *Queue) NewDBInQueue(txn Transaction, queueName string, dbId uint32) (err error) {
	var ret C.int
	var db Db

	ret = C.db_create(&db.ptr, globalEnv.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	if globalEnvConfig.QExtentsize != 0 {
		ret = C.db_set_q_extentsize(db.ptr, C.u_int32_t(globalEnvConfig.QExtentsize))
		if ret != 0 {
			err = DbErrNo(ret)
			return
		}
	}

	ret = C.db_set_pagesize(db.ptr, C.u_int32_t(globalEnvConfig.PageSize))
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	var tmpName *C.char = C.CString(fmt.Sprintf("%s_%d", queueName, dbId))
	defer func() {
		C.free(unsafe.Pointer(tmpName))
	}()

	ret = C.db_open(db.ptr, txn.ptr, tmpName, nil, C.DB_RECNO, C.DB_CREATE, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	if _, ok := queue.dbs[dbId]; ok {
		l4g.Error("db id [%lld] has existed in queue [%s].", dbId, queueName)
		err = errors.New(fmt.Sprintf("db id [%d] has existed in queue [%s].", dbId, queueName))
		return
		//C.db_close(queue.dbs[rid].ptr, 0)
		//delete(queue.dbs, rid)
	}

	queue.dbNames[dbId] = fmt.Sprintf("%s_%d", queueName, dbId)
	queue.dbs[dbId] = db

	return
}

func DelCursor(key string) (err error) {

	var ret C.int
	var queueName string
	if strings.Contains(key, QUEUE_NAME_DELIMITER) {
		strs := strings.Split(key, QUEUE_NAME_DELIMITER)
		if len(strs) != 2 {
			err = errors.New("invalid key")
			return
		}

		queueName = strs[0]
	} else {
		queueName = key
	}

	DumpQstatsToDb()
	globalQCursorListMapRWLock.Lock()

	var ok bool
	if _, ok = globalCursorQlistMap[key]; ok {
		delete(globalCursorQlistMap, key)
	}

	globalQCursorListMapRWLock.Unlock()

	globalQlistMapRWLock.Lock()
	defer func() {
		globalQlistMapRWLock.Unlock()
	}()

	var txn = Transaction{}
	ret = C.db_env_txn_begin(globalEnv.ptr, &txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	defer func() {
		if err != nil {
			C.db_txn_abort(txn.ptr)
		}
	}()

	if ok {
		var dbKey C.DBT
		var keyByte = []byte(key)
		dbKey.data = unsafe.Pointer(&keyByte[0])
		dbKey.size = C.u_int32_t(len(keyByte))
		ret = C.db_del(globalQCursorListDb.ptr, txn.ptr, &dbKey, 0)
		if ret != 0 {
			l4g.Error("db_del err:%s,key:%s", err, key)
			err = DbErrNo(ret)
			return
		}
	}

	qLen := len(queueName)
	for k, _ := range globalCursorQlistMap {
		kLen := len(k)
		if kLen >= qLen && k[:qLen] == queueName {
			//some clients still on, return
			ret = C.db_txn_commit(txn.ptr, 0)
			if ret != 0 {
				err = DbErrNo(ret)
				return
			}

			return
		}
	}

	var queue *Queue
	queue, ok = globalQlistMap[queueName]
	if !ok {
		ret = C.db_txn_commit(txn.ptr, 0)
		if ret != 0 {
			err = DbErrNo(ret)
			return
		}

		return
	}

	for k, v := range queue.dbs {
		ret = C.db_close(v.ptr, 0)
		if ret != 0 {
			err = DbErrNo(ret)
			return
		}

		dbName := C.CString(queue.dbNames[k])
		ret = C.db_env_dbremove(globalEnv.ptr, txn.ptr, dbName, nil, 0)
		C.free(unsafe.Pointer(dbName))
		if ret != 0 {
			err = DbErrNo(ret)
			return
		}
	}

	var dbKey C.DBT
	var keyByte = []byte(queueName)
	dbKey.data = unsafe.Pointer(&keyByte[0])
	dbKey.size = C.u_int32_t(len(keyByte))
	ret = C.db_del(globalQListDb.ptr, txn.ptr, &dbKey, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	ret = C.db_txn_commit(txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	delete(globalQlistMap, queueName)
	return
}

func DumpQstatsToDb() (err error) {

	l4g.Debug("start to dump stats to db")
	var ret C.int

	globalQlistMapRWLock.RLock()
	var qStatsMap = make(map[string]QStats)

	for k, q := range globalQlistMap {
		if q != nil {
			q.mutex.Lock()
			if q.oldSetHits == q.setHits && q.oldGetHits == q.setHits {
				q.mutex.Unlock()
				continue
			}

			q.oldSetHits = q.setHits
			q.oldGetHits = q.getHits
			q.mutex.Unlock()
			qStats := QStats{
				setHits: q.oldSetHits,
				getHits: q.oldGetHits,
			}

			qStatsMap[k] = qStats
		}
	}

	globalQlistMapRWLock.RUnlock()
	var txn = Transaction{}
	ret = C.db_env_txn_begin(globalEnv.ptr, &txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	defer func() {
		if err != nil {
			C.db_txn_abort(txn.ptr)
		}
	}()

	var dbKey, dbData C.DBT
	for k, v := range qStatsMap {
		var keyByte = []byte(k)
		dbKey.data = unsafe.Pointer(&keyByte[0])
		dbKey.size = C.u_int32_t(len(keyByte))
		var valueByte = make([]byte, 8)
		binary.BigEndian.PutUint32(valueByte[0:4], uint32(v.setHits))
		binary.BigEndian.PutUint32(valueByte[4:], uint32(v.getHits))
		dbData.data = unsafe.Pointer(&valueByte[0])
		dbData.size = 8
		ret = C.db_put(globalQListDb.ptr, txn.ptr, &dbKey, &dbData, 0)
		if ret != 0 {
			err = DbErrNo(ret)
			return
		}
		l4g.Debug("dump queue:%s setHitts:%d,getHitts:%d to db", k, v.setHits, v.getHits)
	}

	ret = C.db_txn_commit(txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	DumpCursorStatsToDb()
	l4g.Debug("end to dump stats to db")
	return
}

func DumpCursorStatsToDb() (err error) {

	l4g.Debug("start to dump cursor stats to db")
	var ret C.int

	var txn = Transaction{}
	ret = C.db_env_txn_begin(globalEnv.ptr, &txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	globalQCursorListMapRWLock.RLock()
	defer func() {
		if err != nil {
			C.db_txn_abort(txn.ptr)
		}
		globalQCursorListMapRWLock.RUnlock()
	}()

	var dbKey, dbData C.DBT
	for k, v := range globalCursorQlistMap {
		var keyByte = []byte(k)
		dbKey.data = unsafe.Pointer(&keyByte[0])
		dbKey.size = C.u_int32_t(len(keyByte))

		var valueByte = make([]byte, 12)
		binary.BigEndian.PutUint32(valueByte[0:4], v.cur)
		binary.BigEndian.PutUint32(valueByte[4:8], v.min)
		binary.BigEndian.PutUint32(valueByte[8:], v.max)
		dbData.data = unsafe.Pointer(&valueByte[0])
		dbData.size = 12
		ret = C.db_put(globalQCursorListDb.ptr, txn.ptr, &dbKey, &dbData, 0)
		if ret != 0 {
			err = DbErrNo(ret)
			return
		}

		l4g.Debug("dump cursor %s stats to db.cur:%d min:%d,max:%d", k, v.cur, v.min, v.max)
	}

	ret = C.db_txn_commit(txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	l4g.Debug("end to dump cursor stats to db")
	return
}

func NewCursor(key string) (cursor *Cursor, err error) {

	cursor = &Cursor{
		mutex: sync.Mutex{},
	}
	globalQCursorListMapRWLock.Lock()
	globalCursorQlistMap[key] = cursor
	globalQCursorListMapRWLock.Unlock()

	var queueName string
	if strings.Contains(key, QUEUE_NAME_DELIMITER) {
		strs := strings.Split(key, QUEUE_NAME_DELIMITER)
		if len(strs) != 2 {
			err = errors.New("invalid key")
			return
		}

		queueName = strs[0]
	} else {
		queueName = key
	}

	globalQlistMapRWLock.RLock()
	var queue *Queue
	var ok bool
	if queue, ok = globalQlistMap[queueName]; ok {
		if queue.setHits > 0 {
			cursor.cur = queue.setHits - 1 //set position to the last second
		} else {
			cursor.cur = 0
		}

	} else {
		cursor.cur = 0
	}

	globalQlistMapRWLock.RUnlock()
	return
}

func GetCursor(key string) (cursor *Cursor, err error) {

	l4g.Debug("start to GetCursor")
	globalQCursorListMapRWLock.RLock()

	var ok bool
	if cursor, ok = globalCursorQlistMap[key]; !ok {
		globalQCursorListMapRWLock.RUnlock()
		cursor, err = NewCursor(key)
		if err != nil {
			l4g.Error("NewCursor err:%s", err)
			return
		}
	} else {
		globalQCursorListMapRWLock.RUnlock()
	}

	l4g.Debug("end to GetCursor")
	return
}

func PrintQueueStats() (res string, err error) {

	globalQlistMapRWLock.RLock()

	var setHit, getHit uint32

	for k, q := range globalQlistMap {
		q.mutex.Lock()
		setHit = q.setHits
		getHit = q.getHits
		q.mutex.Unlock()
		res = res + fmt.Sprintf("STAT %s %d/%d\r\n", k, setHit, getHit)
	}

	globalQlistMapRWLock.RUnlock()
	res = res + "END"
	return
}

func CheckPointTick() {
	var duration = time.Duration(globalEnvConfig.CheckpointVal) * time.Second
	timer := time.Tick(duration)
	for {
		select {
		case _ = <-timer:
			l4g.Debug("start to CheckPointTick")
			var ret C.int = C.db_lock_detect(globalEnv.ptr, 0, C.DB_LOCK_YOUNGEST)
			if ret != 0 {
				err := DbErrNo(ret)
				l4g.Error("CheckPointTick err:%s", err)
			}
			l4g.Debug("end to CheckPointTick")
		case _ = <-globalDaemonQuitChan:
			break
		}
	}
}

func CheckMempoolTrickleTick() {
	var duration = time.Duration(globalEnvConfig.MempoolTrickleVal) * time.Second
	timer := time.Tick(duration)
	for {
		select {
		case _ = <-timer:
			l4g.Debug("start to CheckMempoolTrickleTick")
			var percent C.int = C.int(globalEnvConfig.MempoolTricklePercent)
			var nwrote C.int
			var ret C.int = C.db_memp_trickle(globalEnv.ptr, percent, &nwrote)
			if ret != 0 {
				l4g.Error("mempool_trickle err:%s", DbErrNo(ret))
			} else {
				l4g.Info("mempool_trickle thread: writing %d dirty pages", nwrote)
			}

			l4g.Debug("end to CheckMempoolTrickleTick")
		}
	}
}

func CheckDeadLockDetectTick() {
	var duration = time.Duration(globalEnvConfig.DeadlockDetectVal) * time.Second
	timer := time.Tick(duration)
	for {
		select {
		case _ = <-timer:
			l4g.Debug("start to CheckDeadLockDetectTick")
			var ret C.int = C.db_lock_detect(globalEnv.ptr, 0, C.DB_LOCK_YOUNGEST)
			if ret != 0 {
				err := DbErrNo(ret)
				l4g.Error("CheckDeadlockDetect err:%s", err)
			}
			l4g.Debug("end to CheckDeadLockDetectTick")
		case _ = <-globalDaemonQuitChan:
			break
		}
	}
}

func QstatsDumpTick() {
	var duration = time.Duration(globalEnvConfig.QstatsDumpVal) * time.Second
	timer := time.Tick(duration)
	for {
		select {
		case _ = <-timer:
			err := DumpQstatsToDb()
			if err != nil {
				l4g.Error("QstatsDumpTick err:%s", err)
			}
		}
	}
}

type DbItem struct {
	Data []byte
}

func DbGet(key string, id uint32) (item DbItem, err error) {

	var ret C.int
	var queueName string
	if strings.Contains(key, QUEUE_NAME_DELIMITER) {
		strs := strings.Split(key, QUEUE_NAME_DELIMITER)
		if len(strs) != 2 {
			err = errors.New("invalid key")
			return
		}

		queueName = strs[0]
	} else {
		queueName = key
	}

	globalQlistMapRWLock.RLock()
	defer func() {
		globalQlistMapRWLock.RUnlock()
	}()

	cursor, err := GetCursor(key)
	if err != nil {
		l4g.Error("GetCursor err:%s", err)
		return
	}

	var queue *Queue
	var ok bool
	if queue, ok = globalQlistMap[queueName]; !ok {
		l4g.Error("not found queue name:%s", queueName)
		err = DB_ERR_NOTFOUND
		return
	}

	txn := Transaction{}
	var dbKey, dbData C.DBT
	var clientId = id
	cursor.mutex.Lock()
	defer func() {
		cursor.mutex.Unlock()
	}()

	if clientId <= 0 {
		//l4g.Debug("clientId is less than o.change it to cursor.cur %d", cursor.cur)
		clientId = cursor.cur
	}

	if clientId > queue.setHits {
		l4g.Error("client id [%d] is larger than queue [%s] setHits[%d],queue getHits [%d]", clientId, queueName, queue.setHits, queue.getHits)
		item.Data = make([]byte, 0)
		return
	}

	dbId := clientId / MAX_ITEM_PER_DBP
	var db Db
	if db, ok = queue.dbs[dbId]; !ok {
		err = errors.New(fmt.Sprintf("db id [%d] is not existed in queue [%s].", dbId, queueName))
		return
	}

	position := uint32(clientId%MAX_ITEM_PER_DBP + 1)
	//l4g.Debug("clientid=%d,position=%d", clientId, position)
	dbKey.data = unsafe.Pointer(&position)
	dbKey.size = C.u_int32_t(unsafe.Sizeof(position))
	ret = C.db_env_txn_begin(globalEnv.ptr, &txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	defer func() {
		if err != nil {
			C.db_txn_abort(txn.ptr)
		}
	}()

	ret = C.db_get(db.ptr, txn.ptr, &dbKey, &dbData, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		l4g.Error("db_get err:%s", err)
		return
	}

	ret = C.db_txn_commit(txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		l4g.Error("db_txn_commit_get err:%s", err)
		return
	}

	item.Data = C.GoBytes(dbData.data, C.int(dbData.size))

	/*
		if item.Data != nil && len(item.Data) != 0 {
			C.free(dbData.data)
		}
	*/
	cursor.cur++
	queue.getHits++
	//l4g.Debug("get suc.key %s,position:%d,queue getHits:%d,cur:%d", key, position, queue.getHits, cursor.cur)

	return
}

func DbSet(key string, item DbItem) (err error) {

	var ret C.int
	var queueName string
	if strings.Contains(key, QUEUE_NAME_DELIMITER) {
		strs := strings.Split(key, QUEUE_NAME_DELIMITER)
		if len(strs) != 2 {
			err = errors.New("invalid key")
			return
		}

		queueName = strs[0]
	} else {
		queueName = key
	}

	var queue *Queue
	var ok bool
	if queue, ok = globalQlistMap[queueName]; !ok {
		queue = &Queue{
			setHits:    0,
			oldSetHits: 0,
			getHits:    0,
			oldGetHits: 0,
			dbs:        make(map[uint32]Db),
			dbNames:    make(map[uint32]string),
			mutex:      sync.Mutex{},
		}

		err = queue.NewQueueAndInsert(queueName)
		if err != nil {
			return
		}

		globalQlistMapRWLock.Lock()
		globalQlistMap[queueName] = queue
		globalQlistMapRWLock.Unlock()
	}

	queue.mutex.Lock()
	defer func() {
		queue.mutex.Unlock()
	}()

	var db = Db{}
	var txn = Transaction{}
	var dbId = queue.setHits / MAX_ITEM_PER_DBP
	if db, ok = queue.dbs[dbId]; !ok {
		ret = C.db_env_txn_begin(globalEnv.ptr, &txn.ptr, 0)
		if ret != 0 {
			err = DbErrNo(ret)
			return
		}

		err = queue.NewDBInQueue(txn, queueName, dbId)
		if err != nil {
			C.db_txn_abort(txn.ptr)
			return
		}

		ret = C.db_txn_commit(txn.ptr, 0)
		if ret != 0 {
			err = DbErrNo(ret)
			return
		}

		db = queue.dbs[dbId]
	}

	ret = C.db_env_txn_begin(globalEnv.ptr, &txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	defer func() {
		if err != nil {
			C.db_txn_abort(txn.ptr)
		}
	}()

	var dbKey, dbData C.DBT

	var position = uint32(queue.setHits%MAX_ITEM_PER_DBP + 1)
	dbKey.data = unsafe.Pointer(&position)
	dbKey.size = C.u_int32_t(unsafe.Sizeof(position))
	dbData.data = unsafe.Pointer(&item.Data[0])
	dbData.size = C.u_int32_t(len(item.Data))
	ret = C.db_put(db.ptr, txn.ptr, &dbKey, &dbData, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	ret = C.db_txn_commit(txn.ptr, 0)
	if ret != 0 {
		err = DbErrNo(ret)
		return
	}

	if position > MAX_ITEM_PER_DBP {
		l4g.Warn("DbSet position [%ld] is bigger than [%ld]", position, MAX_ITEM_PER_DBP)
	}

	queue.setHits++
	//l4g.Debug("set suc.key %s, position=%d,queue setHitts=%d", key,position, queue.setHits)

	return

}

func GetAllCursorInfo() (res string) {
	globalQCursorListMapRWLock.RLock()
	defer func() {
		globalQCursorListMapRWLock.RUnlock()
	}()

	globalQlistMapRWLock.RLock()
	for k, v := range globalCursorQlistMap {
		max := v.max
		strs := strings.Split(k, QUEUE_NAME_DELIMITER)
		if len(strs) > 0 {
			if queue, ok := globalQlistMap[strs[0]]; ok {
				max = queue.setHits
			}
		}

		res = res + fmt.Sprintf("STAT %s %d/%d\r\n", k, max, v.cur)
	}

	globalQlistMapRWLock.RUnlock()
	res += "END"
	return
}
