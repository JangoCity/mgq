package main

import (
	"flag"
	"fmt"
	"github.com/iamyh/mgq/bdb"
	"github.com/iamyh/mgq/service"
	"time"
	"sync"
	"runtime"
	"log"
	"os"
	"syscall"
	"os/signal"
	l4g "code.google.com/p/log4go"
)

var (
	logConfigFile = flag.String("c", "/Users/suyuhui/Work/goWork/src/github.com/iamyh/mgq/conf/log.xml", "l4g config file,default is /Users/suyuhui/Work/goWork/src/github.com/iamyh/mgq/conf/log.xml")
	port = flag.Int("p", 22201, "TCP port number to listen on (default: 22201)")
	recordSlowLogminValue = flag.Int("n", 1000, "<millisec> minimum value to record slowlog, default is 1000ms")
	cacheSize = flag.Int("m", 64, "in-memmory cache size of BerkeleyDB in megabytes, default is 64MB")
	pageSize = flag.Int("A", 4096, "underlying page size in bytes, default is 4096, (512B ~ 64KB, power-of-two)")
	home = flag.String("H", "/Users/suyuhui/Work/goWork/src/github.com/iamyh/mgq/data", "env home of database, default is '/data1/memcacheq'")
	logBufSize = flag.Int("L", 32, "log buffer size in kbytes, default is 32KB")
	checkPointValue = flag.Int("C", 5 * 60, "do checkpoint every <num> seconds, 0 for disable, default is 5 minutes")
	memTrickleValue = flag.Int("T", 30, "do memp_trickle every <num> seconds, 0 for disable, default is 30 seconds")
	dumpStatsValue = flag.Int("S", 30, "do queue stats dump every <num> seconds, 0 for disable, default is 30 seconds")
	percentUsageCache = flag.Int("e", 60, "percent of the pages in the cache that should be clean, default is 60%%")
	pagesPerDb = flag.Int("E", 16 * 1024, "how many pages in a single db file, default is 16*1024, 0 for disable")
	msgLen = flag.Int("B", 1024, "specify the message body length in bytes, default is 1024")
	deadLockCheckValue = flag.Int("D", 100, "specify the message body length in bytes, default is 1024")
	dbTxnNoSync = flag.Bool("N", false, "enable DB_TXN_NOSYNC to gain big performance improved, default is off")
	autoRemoveLog = flag.Bool("R", false, "automatically remove log files that are no longer needed, default is off")
	maxLocks = flag.Int("l", 40000, "max locker for bdb env, default is 40000")
	globalSettings = Settings{}
	globalStats = Stats{}
	globalStatsLock = sync.Mutex{}
)

type Settings struct {
	Port	int
	Verbose	bool
	MaxDbpsPerQueue int64
	MaxItemsPerQueue int64
	EnableSetCmd bool
	SlowlogMinTime int
}

type Stats struct {
	CurrConns uint
	TotalConns uint
	ConnStructs uint
	GetCmds uint64
	GetHits uint64
	SetCmds uint64
	SetHits uint64
	Started time.Time
	BytesRead uint64
	BytesWritten uint64
}

func initStats() {
	globalStats.Started = time.Now()
}

func resetStats() {
	globalStatsLock.Lock()
	globalStats.TotalConns = 0
	globalStats.GetCmds = 0
	globalStats.SetCmds = 0
	globalStats.BytesRead = 0
	globalStats.BytesWritten = 0
	globalStatsLock.Unlock()
}

func initConfig() {
	flag.Parse()
	l4g.LoadConfiguration(*logConfigFile)
	globalSettings.Port = *port;
	globalSettings.SlowlogMinTime = *recordSlowLogminValue
	envConfig := bdb.EnvConfig{}
	envConfig.Home = *home
	envConfig.CacheSize = int32(*cacheSize * 1024 * 1024)
	envConfig.TxnNoSync = *dbTxnNoSync
	envConfig.TxLogBufferSize = int32(*logBufSize * 1024)
	envConfig.AutoRemoveLog = *autoRemoveLog
	envConfig.PageSize = int32(*pageSize)
	envConfig.MaxLock = int32(*maxLocks)
	envConfig.DeadlockDetectVal = *deadLockCheckValue
	envConfig.CheckpointVal = *checkPointValue
	envConfig.MempoolTrickleVal = *memTrickleValue
	envConfig.MempoolTricklePercent = *percentUsageCache
	envConfig.QstatsDumpVal = *dumpStatsValue
	envConfig.ReLen = int32(*msgLen)
	envConfig.QExtentsize = int32(*pagesPerDb)
	bdb.InitBdbEnv(envConfig)
	return
}

func usage() {
	fmt.Println("-p <num>      TCP port number to listen on (default: 22201)")
	fmt.Println("-U <num>      UDP port number to listen on (default: 0, off)")
	fmt.Println("-s <file>     unix socket path to listen on (disables network support)")
	fmt.Println("-a <mask>     access mask for unix socket, in octal (default 0700)")
	fmt.Println("-l <ip_addr>  interface to listen on, default is INDRR_ANY")
	fmt.Println("-u <username> assume identity of <username> (only when run as root)")
	fmt.Println("-c <num>      max simultaneous connections, default is 1024")
	fmt.Println("-v            verbose (print errors/warnings while in event loop)")
	fmt.Println("-vv           very verbose (also print client commands/reponses)")
	fmt.Println("-h            print this help and exit")
	fmt.Println("-i            print license info")
	fmt.Println("-M <num>      number of max dbp file per queue, default 10 (10*1024*1024 items per queue)");
	fmt.Println("-n <millisec> minimum value to record slowlog, default is 1000ms");

	fmt.Println("--------------------BerkeleyDB Options-------------------------------\n");
	fmt.Println("-m <num>      in-memmory cache size of BerkeleyDB in megabytes, default is 64MB");
	fmt.Println("-A <num>      underlying page size in bytes, default is 4096, (512B ~ 64KB, power-of-two)");
	fmt.Println("-H <dir>      env home of database, default is '/data1/memcacheq'");
	fmt.Println("-L <num>      log buffer size in kbytes, default is 32KB");
	fmt.Println("-C <num>      do checkpoint every <num> seconds, 0 for disable, default is 5 minutes");
	fmt.Println("-T <num>      do memp_trickle every <num> seconds, 0 for disable, default is 30 seconds");
	fmt.Println("-S <num>      do queue stats dump every <num> seconds, 0 for disable, default is 30 seconds");
	fmt.Println("-e <num>      percent of the pages in the cache that should be clean, default is 60%%");
	/* queue only */
	fmt.Println("-E <num>      how many pages in a single db file, default is 16*1024, 0 for disable");
	fmt.Println("-B <num>      specify the message body length in bytes, default is 1024");

	fmt.Println("-D <num>      do deadlock detecting every <num> millisecond, 0 for disable, default is 100ms");
	fmt.Println("-N            enable DB_TXN_NOSYNC to gain big performance improved, default is off");
	fmt.Println("-R            automatically remove log files that are no longer needed, default is off");
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	initConfig()
	initStats()
	config := service.TcpConfig{
		Port: globalSettings.Port       ,
		SendLimit:    88,
		ReceiveLimit: 88,
	}

	srv := service.NewService(config, &service.MgqProtocolImpl{}, &service.ConnCallBack{})
	var err error
	if os.Getenv("YMTCP_RESTART") == "true" {
		err = srv.InitFromFD(3)
	} else {
		err = srv.Init()
	}

	if err != nil {
		log.Fatalf("tcp service init err:%s", err)
	}

	go srv.Service()
	log.Println("listen:", config.Port)

	chanSignal := make(chan os.Signal)
	signal.Notify(chanSignal, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for receiveSignal := range chanSignal {
		if receiveSignal == syscall.SIGTERM {

			log.Println("receive signal SIGTERM,stop service.pid:", os.Getpid())
			srv.StopAccept()
			srv.WaitAndDone()
			os.Exit(0)
		} else {
			log.Println("receive signal SIGHUP")

			//停止接受新连接
			srv.StopAccept()
			fd, err := srv.GetListenerFD()
			if err != nil {
				log.Fatalf("get tcp service fd err:%s", err)
			}

			os.Setenv("YMTCP_RESTART", "true")

			execSpec := &syscall.ProcAttr{
				Env: os.Environ(),
				Files: []uintptr{os.Stdin.Fd(), os.Stdout.Fd(), os.Stderr.Fd(), fd},
			}

			fork, err := syscall.ForkExec(os.Args[0], os.Args, execSpec)
			if err != nil {
				log.Fatalln("Fail to fork", err)
			}

			log.Printf("pid:%d restart graceful,new pid:%d start", os.Getpid(), fork)
			srv.WaitAndDone()
			os.Exit(0)
		}
	}
}
