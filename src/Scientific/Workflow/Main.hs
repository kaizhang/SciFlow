{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE RecordWildCards #-}

module Scientific.Workflow.Main
    ( mainWith
    , defaultMain
    , defaultMainOpts
    , RunMode(..)
    ) where

import Path
import           Control.Exception.Safe (MonadMask)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Control (MonadBaseControl)
import qualified Control.Funflow.ContentStore                as CS
import System.Directory (makeAbsolute)
import System.Environment

import Scientific.Workflow.Exec
import Scientific.Workflow.Coordinator
import Scientific.Workflow.Coordinator.Drmaa
import Scientific.Workflow.Types

data MainOpts = MainOpts
    { _store_path :: FilePath
    , _run_mode :: RunMode
    , _host_ip :: (Int, Int, Int, Int)
    , _host_port :: Int
    }

data RunMode = Normal
             | Master
             | Slave

defaultMainOpts :: MainOpts
defaultMainOpts = MainOpts
    { _store_path = "sciflow_db"
    , _run_mode = Normal
    , _host_ip = (192, 168, 0, 1)
    , _host_port = 8888
    }

defaultMain :: (MonadMask m, MonadIO m, MonadBaseControl IO m)
            => MainOpts -> SciFlow m a b -> a -> m b
defaultMain MainOpts{..} wf input = do
    args <- liftIO $ getArgs
    exePath <- liftIO $ getExecutablePath
    let config = DrmaaConfig
            { _queue_size = 5
            , _cmd = (exePath, ["--slave"])
            , _address = _host_ip
            , _port = _host_port }
    case args of
        ["--slave"] -> mainWith Slave _store_path config wf input
        _ -> mainWith Master _store_path config wf input

mainWith :: (MonadMask m, MonadIO m, MonadBaseControl IO m)
         => RunMode
         -> FilePath
         -> DrmaaConfig
         -> SciFlow m a b
         -> a
         -> m b
mainWith runMode p config wf input = do
    dir <- liftIO $ makeAbsolute p >>= parseAbsDir
    res <- CS.withStore dir $ \store -> case runMode of
        Normal -> runSciFlow (Nothing :: Maybe (Connection Drmaa)) store wf input
        Master -> withDrmaa config $ \d -> runCoordinator d store wf input
        Slave -> withConnection config $ \conn -> runSciFlow (Just conn) store wf input
    case res of
        Left err -> error $ "Something went wrong: " ++ show err
        Right x -> return x