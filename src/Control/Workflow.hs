{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}

module Control.Workflow
    ( mainWith
    , defaultMainOpts
    , MainOpts(..)
    , initClient
    , module Control.Workflow.Language
    , module Control.Workflow.Types
    ) where

import Path
import qualified Data.ByteString.Char8                             as B
import qualified Control.Funflow.ContentStore                as CS
import           Control.Arrow.Async
import System.Directory (makeAbsolute)
import System.Environment
import Data.Binary
import Control.Distributed.Process hiding (bracket)
import           Control.Exception.Safe                      (bracket)
import Control.Distributed.Process.Node
import           System.IO                                   (stderr)
import GHC.Conc (atomically)
import Control.Concurrent (threadDelay)
import Control.Monad.Except (runExceptT)
import           Katip
import Network.Transport (EndPointAddress(..), Transport)
import Network.HostName (getHostName)
import Network.Transport.TCP (createTransport, defaultTCPAddr, defaultTCPParameters)

import Control.Workflow.Interpreter.Exec
import Control.Workflow.Coordinator.Drmaa
import Control.Workflow.Language
import Control.Workflow.Types

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
        port = show _master_port
        config = DrmaaConfig
            { _queue_size = _n_workers
            , _cmd = (exePath, ["--slave"])
            , _address = _master_addr
            , _port = _master_port }
    getArgs >>= \case
        ["--slave"] -> initClient (masterAddr config) (masterPort config) $ _function_table wf
        _ -> withDrmaa config $ \drmaa -> do
            Right transport <- createTransport (defaultTCPAddr host port)
                defaultTCPParameters
            dir <- makeAbsolute _store_path >>= parseAbsDir
            CS.withStore dir $ \store -> 
                runSciFlow drmaa transport store env wf


-- | Simple evaulation of a flow
runSciFlow :: (Coordinator coordinator, Binary env)
           => coordinator
           -> Transport
           -> CS.ContentStore
           -> env
           -> SciFlow env
           -> IO ()
runSciFlow coord transport store env sciflow = do
    handleScribe <- mkHandleScribe ColorIfTerminal stderr InfoS V2
    let mkLogEnv = registerScribe "stderr" handleScribe defaultScribeSettings =<<
            initLogEnv "SciFlow" "production"
    bracket mkLogEnv closeScribes $ \le -> do
        let initialContext = ()
            initialNamespace = "executeLoop"
        nd <- newLocalNode transport $ _rtable $ _function_table sciflow
        _ <- forkProcess nd $ do
            getSelfPid >>= register "SciFlow_master"
            initServer coord
        runProcess nd $ runExceptT
            ( runKatipContextT le initialContext initialNamespace $
                runAsyncA (execFlow coord store env sciflow) ()
            ) >>= \case
                Left ex -> error $ show ex
                Right _ -> shutdown
  where
    shutdown = do
        liftIO $ putStrLn "shutdown all workers"
        workers <- liftIO $ atomically $ getWorkers coord
        mapM_ (remove coord . _worker_id) workers
        liftIO $ threadDelay 1000000 >> putStrLn "Finished!"

initServer :: Coordinator coordinator
           => coordinator
           -> Process ()
initServer coord = do
    worker <- expect :: Process Worker
    liftIO $ do
        putStrLn $ "Found a new worker: " ++ show (_worker_id worker)
        atomically $ addToQueue coord worker
    initServer coord

shutdownServer :: Coordinator coordinator
               => coordinator
               -> Process ()
shutdownServer = undefined

initClient :: String   -- ^ The address of Master node 
           -> Int      -- ^ The port of Master node
           -> FunctionTable
           -> IO ()
initClient addr port rf = do
    host <- getHostName
    transport <- tryCreateTransport host ([8000..8200] :: [Int])
    node <- newLocalNode transport $ _rtable rf
    runProcess node $ do
        let serverAddr = NodeId $ EndPointAddress $ B.intercalate ":" $
                [B.pack addr, B.pack $ show port, "0"]
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