{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase    #-}

module Control.Workflow.Coordinator.Drmaa
    ( Drmaa
    , DrmaaConfig(..)
    , mainWith
    ) where

import           Control.Monad.IO.Class                      (liftIO)
import Control.Monad (replicateM)
import Network.HostName (getHostName)
import qualified Data.HashMap.Strict as M
import qualified DRMAA as D
import qualified Data.ByteString.Char8                             as B
import qualified Control.Funflow.ContentStore                as CS
import Data.Binary (Binary)
import Control.Distributed.Process
import Data.List (foldl')
import Control.Concurrent.STM
import Control.Concurrent (threadDelay)
import Control.Monad (forever)
import System.Environment (getArgs, getExecutablePath, getEnv)
import Network.Transport.TCP (createTransport, defaultTCPAddr, defaultTCPParameters)
import Control.Distributed.Process.Node
import Path (parseAbsDir)
import Network.Transport (EndPointAddress(..))
import System.Directory (makeAbsolute)
import Text.Printf (printf)
import Data.Maybe (fromMaybe)
import System.Random (randomRIO)

import Control.Workflow.Coordinator
import Control.Workflow
import Control.Workflow.Interpreter.Exec

mainWith :: Binary env
         => SciFlowOpts
         -> env
         -> SciFlow env
         -> IO ()
mainWith SciFlowOpts{..} env wf = do
    exePath <- getExecutablePath
    let host = _master_addr
        port = _master_port
        config = DrmaaConfig
            { _queue_size = _n_workers
            , _cmd = (exePath, ["--slave"])
            , _address = host
            , _port = port
            , _cpu_format = "--ntasks-per-node=%d" 
            , _memory_format = "--mem=%d000"
            , _queue_format = "-p %s"
            , _drmaa_parameters = Nothing }
    withCoordinator config $ \drmaa -> getArgs >>= \case
        ["--slave"] -> startClient drmaa $ _function_table wf
        _ -> do
            Right transport <- createTransport (defaultTCPAddr host (show port))
                defaultTCPParameters
            dir <- makeAbsolute _store_path >>= parseAbsDir
            CS.withStore dir $ \store -> 
                runSciFlow drmaa transport store _resources env wf

data DrmaaConfig = DrmaaConfig
    { _queue_size :: Int
    , _cmd :: (FilePath, [String])
    , _address :: String
    , _port :: Int
    , _cpu_format :: String   -- ^ How to specify cpu number, default: "--ntasks-per-node=%d"
    , _memory_format :: String   -- ^ How to specify memory, default: "--mem=%dG"
    , _queue_format :: String
    , _drmaa_parameters :: Maybe String -- ^ additional drmaa parameters
    }

data Drmaa = Drmaa
    { _worker_pool :: TMVar WorkerPool
    , _config :: DrmaaConfig }

instance Coordinator Drmaa where
    type Config Drmaa = DrmaaConfig

    withCoordinator config f = D.withSession $ liftIO drmaa >>= f
      where
        drmaa = Drmaa <$> newTMVarIO (WorkerPool 0 M.empty) <*> return config

    initiate coord = do
        getSelfPid >>= register "SciFlow_master"
        -- Kill idle worker periodically.
        forever $ liftIO (threadDelay 5000000) >> killIdleWorkers
      where
        killIdleWorkers = liftIO (atomically getIdleWorkers) >>= \case
            Nothing -> return ()
            Just (workers, pool) -> do
                mapM_ (flip send Shutdown) workers
                liftIO $ atomically $ putTMVar (_worker_pool coord) $
                    foldl' (flip removeWorker) pool workers
        getIdleWorkers = filter ((==Idle) . _worker_status) <$>
            getWorkers coord >>= \case
                [] -> return Nothing
                x -> do
                    pool <- takeTMVar $ _worker_pool coord
                    return $ Just (map _worker_id x, pool)

    shutdown coord = do
        liftIO $ putStrLn "shutdown all workers"
        liftIO (atomically $ getWorkers coord) >>=
            mapM_ (\worker -> send (_worker_id worker) Shutdown)
        liftIO $ threadDelay 1000000

    startClient Drmaa{..} rf = do
        host <- getHostName
        transport <- tryCreateTransport host ([8000..8200] :: [Int])
        node <- newLocalNode transport $ _rtable rf
        runProcess node $ do
            let serverAddr = NodeId $ EndPointAddress $ B.intercalate ":" $
                    [B.pack $ _address _config, B.pack $ show $ _port _config, "0"]
            -- Link to the main process
            linkNode serverAddr
            serverPid <- liftIO (getEnv "master_id") >>= searchServer serverAddr
            getSelfPid >>= send serverPid
            (expect :: Process Signal) >>= \case
                Shutdown -> return ()
      where
        searchServer :: NodeId -> String -> Process ProcessId
        searchServer server name = do
            whereisRemoteAsync server name
            expectTimeout 1000000 >>= \case
                Just (WhereIsReply _ (Just sid)) -> return sid
                _ -> liftIO (putStrLn "Server not found") >> terminate
        tryCreateTransport host (p:ports) = createTransport 
            (defaultTCPAddr host (show p)) defaultTCPParameters >>= \case
                Left _ -> tryCreateTransport host ports
                Right trsp -> return trsp
        tryCreateTransport _ _ = error "Failed to create transport"

    getWorkers Drmaa{..} = M.elems . _cur_workers <$> readTMVar _worker_pool

    reserve coord@Drmaa{..} wc = tryReserve >>= \case
        -- Queue is full
        Nothing -> liftIO (threadDelay 1000000) >> reserve coord wc
        -- Idle worker exist
        Just (Right nd) -> return nd
        -- Try to get a new worker
        Just (Left pool) -> do
            liftIO $ atomically $ putTMVar _worker_pool $
                pool{_len_waitlist = _len_waitlist pool + 1}
            worker <- spawnWorker _config wc
            liftIO $ atomically $ do
                pool' <- addWorker (_worker_id worker)
                    worker{_worker_status = Working } <$> takeTMVar _worker_pool
                putTMVar _worker_pool $
                    pool'{_len_waitlist = _len_waitlist pool' - 1}
            liftIO $ putStrLn $ "Found a new worker: " ++ show (_worker_id worker)
            return $ _worker_id worker
      where
        -- Try reserving a work, return the worker id if succeed; return Nothing
        -- if no worker is available; return the coordinator lock if a new worker
        -- is going to be spawned.
        tryReserve = liftIO $ atomically $ do
            workers <- getWorkers coord
            waiting <- _len_waitlist <$> readTMVar _worker_pool
            case filter isQualified workers of
                (w:_) -> do
                    setWorkerStatus coord (_worker_id w) Working
                    return $ Just $ Right $ _worker_id w
                [] -> if length workers + waiting < _queue_size _config
                    then Just . Left <$> takeTMVar _worker_pool
                    else return Nothing
        isQualified Worker{..} = _worker_status == Idle && wc == _worker_config
   
    freeWorker coord worker = liftIO $ atomically $ setWorkerStatus coord worker Idle

data WorkerPool = WorkerPool
    { _len_waitlist :: Int
    , _cur_workers :: M.HashMap ProcessId Worker }

addWorker :: ProcessId ->  Worker -> WorkerPool -> WorkerPool
addWorker pid worker (WorkerPool n p) = WorkerPool n $ M.insert pid worker p
{-# INLINE addWorker #-}

removeWorker :: ProcessId -> WorkerPool -> WorkerPool
removeWorker pid (WorkerPool n p) = WorkerPool n $ M.delete pid p
{-# INLINE removeWorker #-}

setStatus :: ProcessId -> WorkerStatus -> WorkerPool -> WorkerPool
setStatus pid status (WorkerPool n p) = WorkerPool n $
    M.adjust (\x -> x {_worker_status = status}) pid p
{-# INLINE setStatus #-}

setWorkerStatus Drmaa{..} host status = (setStatus host status <$> takeTMVar _worker_pool) >>=
    putTMVar _worker_pool

spawnWorker :: DrmaaConfig -> Maybe Resource -> Process Worker
spawnWorker config wc = do
    procName <- liftIO $ replicateM 16 $ randomRIO ('a', 'z')
    getSelfPid >>= register procName
    let attr = D.defaultJobAttributes
            { D._env = [("master_id", procName)]
            , D._native_specification = Just paras }
    liftIO $ D.runJob exe args attr >>= \case
        Left err -> error err
        Right _ -> return ()
    pid <- expect 
    return $ Worker pid Idle wc
  where
    (exe, args) = _cmd config
    cpu = fromMaybe [] $ fmap (return . printf (_cpu_format config)) $
        wc >>= _num_cpu
    mem = fromMaybe [] $ fmap (return . printf (_memory_format config)) $
        wc >>= _total_memory 
    q = fromMaybe [] $ fmap (return . printf (_queue_format config)) $
        wc >>= _submit_queue
    paras = unwords $ maybe [] return (_drmaa_parameters config) ++ cpu ++ mem ++ q
{-# INLINE spawnWorker #-}