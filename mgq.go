//Author: tab@iyouya.com

package main

import (
	"flag"
	"fmt"
	"github.com/YoungPioneers/mgq/bdb"
	"github.com/YoungPioneers/mgq/service"
	l4g "github.com/alecthomas/log4go"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var (
	logConfigFile         = flag.String("c", "../conf/log.xml", "l4g config file,default is $ROOT_PATH/conf/log.xml")
	port                  = flag.Int("p", 22201, "TCP port number to listen on (default: 22201)")
	recordSlowLogminValue = flag.Int("n", 1, "<millisec> minimum value to record slowlog, default is 1s")
	action                = flag.String("s", "start", "start|stop|restart")
	cacheSize             = flag.Int("m", 64, "in-memmory cache size of BerkeleyDB in megabytes, default is 64MB")
	pageSize              = flag.Int("A", 4096, "underlying page size in bytes, default is 4096, (512B ~ 64KB, power-of-two)")
	home                  = flag.String("H", "../data", "env home of database, default is '$ROOT_PATH/data'")
	logBufSize            = flag.Int("L", 32, "log buffer size in kbytes, default is 32KB")
	checkPointValue       = flag.Int("C", 30, "do checkpoint every <num> seconds, default is 30")
	memTrickleValue       = flag.Int("T", 30, "do memp_trickle every <num> seconds, default is 30 seconds")
	dumpStatsValue        = flag.Int("S", 30, "do queue stats dump every <num> seconds, default is 30 seconds")
	percentUsageCache     = flag.Int("e", 30, "percent of the pages in the cache that should be clean, default is 60%%")
	pagesPerDb            = flag.Int("E", 16*1024, "how many pages in a single db file, default is 16*1024")
	msgLen                = flag.Int("B", 1024, "specify the message body length in bytes, default is 1024")
	deadLockCheckValue    = flag.Int("D", 1, "do deadlock detecting every <num> millisecond, default is 1s")
	dbTxnNoSync           = flag.Bool("N", false, "enable DB_TXN_NOSYNC to gain big performance improved, default is false")
	autoRemoveLog         = flag.Bool("R", false, "automatically remove log files that are no longer needed, default is false")
	maxLocks              = flag.Int("l", 40000, "max locker for bdb env, default is 40000")
	globalSettings        = Settings{}
	globalStats           = Stats{}
)

type Settings struct {
	Port             int
	Verbose          bool
	MaxDbpsPerQueue  int64
	MaxItemsPerQueue int64
	EnableSetCmd     bool
	SlowlogMinTime   int
}

type Stats struct {
	CurrConns    uint
	TotalConns   uint
	ConnStructs  uint
	GetCmds      uint64
	GetHits      uint64
	SetCmds      uint64
	SetHits      uint64
	Started      time.Time
	BytesRead    uint64
	BytesWritten uint64
}

func initStats() {
	globalStats.Started = time.Now()
}

func initConfig() {
	flag.Parse()
	l4g.LoadConfiguration(*logConfigFile)
	globalSettings.Port = *port
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
	fmt.Println("-p=<num>      TCP port number to listen on (default: 22201)")
	fmt.Println("-h            print this help and exit")
	fmt.Println("-n=<millisec> minimum value to record slowlog, default is 1s")
	fmt.Println("-s=<action>   start|stop|restart")

	fmt.Println("--------------------BerkeleyDB Options-------------------------------\n")
	fmt.Println("-m=<num>      in-memmory cache size of BerkeleyDB in megabytes, default is 64MB")
	fmt.Println("-l=<num>      max locker for bdb env, default is 40000")
	fmt.Println("-A=<num>      underlying page size in bytes, default is 4096, (512B ~ 64KB, power-of-two)")
	fmt.Println("-H=<dir>      env home of database, default is '$ROOT_PATH/data'")
	fmt.Println("-L=<num>      log buffer size in kbytes, default is 32KB")
	fmt.Println("-C=<num>      do checkpoint every <num> seconds, default is 30 seconds")
	fmt.Println("-T=<num>      do memp_trickle every <num> seconds, default is 30 seconds")
	fmt.Println("-S=<num>      do queue stats dump every <num> seconds, default is 30 seconds")
	fmt.Println("-e=<num>      percent of the pages in the cache that should be clean, default is 60%%")
	/* queue only */
	fmt.Println("-E=<num>      how many pages in a single db file, default is 16*1024, 0 for disable")
	fmt.Println("-B=<num>      specify the message body length in bytes, default is 1024")

	fmt.Println("-D=<num>      do deadlock detecting every <num> millisecond,default is 1")
	fmt.Println("-N=<num>      enable DB_TXN_NOSYNC to gain big performance improved, default is 0")
	fmt.Println("-R=<num>      automatically remove log files that are no longer needed, default is 0")
}

func main() {
	if len(os.Args) != 0 {
		for _, arg := range os.Args {
			if arg == "-h" {
				usage()
				return
			}
		}
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	initConfig()
	initStats()
	config := service.TcpConfig{
		Port:         globalSettings.Port,
		SendLimit:    65535,
		ReceiveLimit: 65535,
	}

	srv := service.NewService(config, &service.MgqProtocolImpl{}, &service.ConnCallBack{})
	var err error
	if os.Getenv("RESTART") == "true" {
		err = srv.InitFromFD(3)
	} else {
		err = srv.Init()
	}

	if err != nil {
		l4g.Error("tcp service init err:%s", err)
	}

	go srv.Service()
	l4g.Info("listen:%d", config.Port)

	chanSignal := make(chan os.Signal)
	signal.Notify(chanSignal, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for receiveSignal := range chanSignal {
		if receiveSignal == syscall.SIGTERM {

			l4g.Warn("receive signal SIGTERM,stop service.pid:%d", os.Getpid())
			srv.StopAccept()
			srv.WaitAndDone()
			os.Exit(0)
		} else {
			l4g.Warn("receive signal SIGHUP")

			//停止接受新连接
			srv.StopAccept()
			/*
				fd, err := srv.GetListenerFD()
				if err != nil {
					l4g.Error("get tcp service fd err:%s", err)
				}

				os.Setenv("RESTART", "true")

				execSpec := &syscall.ProcAttr{
					Env:   os.Environ(),
					Files: []uintptr{os.Stdin.Fd(), os.Stdout.Fd(), os.Stderr.Fd(), fd},
				}

				fork, err := syscall.ForkExec(os.Args[0], os.Args, execSpec)
				if err != nil {
					l4g.Error("Fail to fork, err:%s", err)
				}

				l4g.Info("pid:%d restart,new pid:%d start", os.Getpid(), fork)
			*/
			srv.WaitAndDone()

			os.Exit(0)
		}
	}
}
