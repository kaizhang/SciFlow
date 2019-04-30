{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase    #-}

module Control.Workflow.Coordinator.Drmaa
    ( Drmaa
    , DrmaaConfig(..)
    , getDefaultDrmaaConfig
    , MainOpts(..)
    , defaultMainOpts
    , mainWith
    ) where

import           Control.Monad.IO.Class                      (liftIO)
import Network.HostName (getHostName)
import qualified Data.HashMap.Strict as M
import qualified DRMAA as D
import qualified Data.ByteString.Char8                             as B
import qualified Control.Funflow.ContentStore                as CS
import Data.Binary (Binary)
import Control.Distributed.Process
import Control.Concurrent.STM
import Control.Concurrent (threadDelay)
import Control.Monad (forever)
import System.Environment (getArgs, getExecutablePath)
import Network.Transport.TCP (createTransport, defaultTCPAddr, defaultTCPParameters)
import Control.Distributed.Process.Node
import Path (parseAbsDir)
import Network.Transport (EndPointAddress(..))
import System.Directory (makeAbsolute)
import Control.Concurrent.MVar

import Control.Workflow.Coordinator
import Control.Workflow.Types
import Control.Workflow.Interpreter.Exec

data DrmaaConfig = DrmaaConfig
    { _queue_size :: Int
    , _cmd :: (FilePath, [String])
    , _address :: String
    , _port :: Int
    -- , _job_parameters :: M.HashMap T.Text JobParas
    }

getDefaultDrmaaConfig :: IO DrmaaConfig
getDefaultDrmaaConfig = do
    exePath <- getExecutablePath
    return $ DrmaaConfig
        { _queue_size = 5
        , _cmd = (exePath, ["--slave"])
        , _address = "192.168.0.1"
        , _port = 8888 }

type WorkerPool = TMVar (M.HashMap ProcessId Worker)

data Drmaa = Drmaa
    { _worker_pool :: WorkerPool
    , _config :: DrmaaConfig
    , _new_worker :: MVar Worker }

instance Coordinator Drmaa where
    type Config Drmaa = DrmaaConfig

    withCoordinator config f = D.withSession $ liftIO ( Drmaa <$>
        newTMVarIO M.empty <*> return config <*> newEmptyMVar ) >>= f

    startServer coord = do
        getSelfPid >>= register "SciFlow_master"
        forever $ do
            worker <- expect :: Process Worker
            liftIO $ putMVar (_new_worker coord) worker

    shutdownServer coord = do
        liftIO $ putStrLn "shutdown all workers"
        workers <- liftIO $ atomically $ getWorkers coord
        mapM_ (remove coord . _worker_id) workers
        liftIO $ threadDelay 1000000

    startClient Drmaa{..} rf = do
        host <- getHostName
        transport <- tryCreateTransport host ([8000..8200] :: [Int])
        node <- newLocalNode transport $ _rtable rf
        runProcess node $ do
            let serverAddr = NodeId $ EndPointAddress $ B.intercalate ":" $
                    [B.pack $ _address _config, B.pack $ show $ _port _config, "0"]
            nd <- searchServer serverAddr
            liftIO $ putStrLn $ "Connected to " ++ show nd
            self <- getSelfPid
            send nd $ Worker self Idle
            (expect :: Process Signal) >>= \case
                Shutdown -> return ()
      where
        searchServer :: NodeId -> Process ProcessId
        searchServer server = do
            whereisRemoteAsync server "SciFlow_master"
            expectTimeout 1000000 >>= \case
                Just (WhereIsReply _ (Just sid)) -> return sid
                _ -> liftIO (putStrLn "Server not found") >> terminate
        tryCreateTransport host (p:ports) = createTransport 
            (defaultTCPAddr host (show p)) defaultTCPParameters >>= \case
                Left _ -> tryCreateTransport host ports
                Right trsp -> return trsp
        tryCreateTransport _ _ = error "Failed to create transport"

    getWorkers Drmaa{..} = M.elems <$> readTMVar _worker_pool

    addToPool Drmaa{..} worker =
        (M.insert (_worker_id worker) worker <$> takeTMVar _worker_pool) >>= 
            putTMVar _worker_pool

    reserve coord@Drmaa{..} = tryReserve >>= \case
        Nothing -> liftIO (threadDelay 1000000) >> reserve coord
        Just (Right nd) -> return nd
        Just (Left pool) -> liftIO $ do
            worker <- spawnWorker _config >> takeMVar _new_worker
            atomically $ putTMVar _worker_pool $
                M.insert (_worker_id worker) worker{_worker_status=Working} pool
            putStrLn $ "Found a new worker: " ++ show (_worker_id worker)
            return $ _worker_id worker
      where
        -- Try reserving a work, return the worker id if succeed; return Nothing
        -- if no worker is available; return the coordinator lock if a new worker
        -- is going to be spawned.
        tryReserve = liftIO $ atomically $ do
            workers <- getWorkers coord
            case filter ((==Idle) . _worker_status) workers of
                (w:_) -> do
                    setWorkerStatus coord (_worker_id w) Working
                    return $ Just $ Right $ _worker_id w
                [] -> if length workers < _queue_size _config
                    then Just . Left <$> takeTMVar _worker_pool
                    else return Nothing
   
    release coord worker = liftIO $ atomically $ setWorkerStatus coord worker Idle

    remove Drmaa{..} worker = do
        send worker Shutdown
        liftIO $ atomically $
            (M.delete worker <$> takeTMVar _worker_pool) >>= putTMVar _worker_pool

-- | Spawn a new worker
spawnWorker :: DrmaaConfig -> IO ()
spawnWorker config = do
    _ <- D.runJob exe args D.defaultJobAttributes {D._env = []}
    return ()
  where
    (exe, args) = _cmd config

setWorkerStatus Drmaa{..} host status =
    (M.adjust (\x -> x {_worker_status = status}) host <$> takeTMVar _worker_pool) >>=
        putTMVar _worker_pool

data MainOpts = MainOpts
    { _store_path :: FilePath
    , _master_addr :: String
    , _master_port :: Int
    , _n_workers :: Int
    }

defaultMainOpts :: MainOpts
defaultMainOpts = MainOpts
    { _store_path = "sciflow_db"
    , _master_addr = "192.168.0.1"
    , _master_port = 8888
    , _n_workers = 5
    }

mainWith :: Binary env
         => MainOpts
         -> env
         -> SciFlow env
         -> IO ()
mainWith MainOpts{..} env wf = do
    exePath <- getExecutablePath
    let host = _master_addr
        port = _master_port
        config = DrmaaConfig
            { _queue_size = _n_workers
            , _cmd = (exePath, ["--slave"])
            , _address = host
            , _port = port }
    withCoordinator config $ \drmaa -> getArgs >>= \case
        ["--slave"] -> startClient drmaa $ _function_table wf
        _ -> do
            Right transport <- createTransport (defaultTCPAddr host (show port))
                defaultTCPParameters
            dir <- makeAbsolute _store_path >>= parseAbsDir
            CS.withStore dir $ \store -> 
                runSciFlow drmaa transport store env wf

