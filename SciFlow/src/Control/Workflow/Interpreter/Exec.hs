{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}

module Control.Workflow.Interpreter.Exec (runSciFlow) where

import           Control.Arrow.Async
import           Control.Arrow.Free                          (eval)
import Control.Monad.Reader
import Control.Monad.Except (ExceptT, throwError, runExceptT)
import           Control.Exception.Safe                      (SomeException,
                                                              handleAny)
import           Control.Funflow.ContentHashable
import qualified Control.Funflow.ContentStore                as CS
import           Control.Monad.IO.Class                      (MonadIO, liftIO)
import           Control.Monad.Trans.Class                   (lift)
import Control.Distributed.Process (kill, processNodeId, call)
import Control.Distributed.Process.Node (forkProcess, runProcess, newLocalNode, LocalNode)
import Control.Distributed.Process.MonadBaseControl ()
import qualified Data.ByteString.Lazy                             as BS
import qualified Data.HashMap.Strict as M
import           Control.Exception.Safe                      (bracket)
import           System.IO                                   (stdout)
import Network.Transport (Transport)
import Data.Binary (Binary(..), encode, decode)
import           Katip
import           Path
import Control.Concurrent.MVar
import qualified Data.Text as T

import Control.Workflow.Types
import Control.Workflow.Coordinator

runSciFlow :: (Coordinator coordinator, Binary env)
           => coordinator       -- ^ Coordinator backend
           -> Transport         -- ^ Cloud Haskell transport
           -> CS.ContentStore   -- ^ Local cache
           -> ResourceConfig    -- ^ Job resource configuration
           -> env               -- ^ Optional environmental variables
           -> SciFlow env
           -> IO ()
runSciFlow coord transport store resource env sciflow = do
    handleScribe <- mkHandleScribe ColorIfTerminal stdout InfoS V0
    let mkLogEnv = registerScribe "stderr" handleScribe defaultScribeSettings =<<
            initLogEnv mempty ""
    bracket mkLogEnv closeScribes $ \le -> do
        nd <- newLocalNode transport $ _rtable $ _function_table sciflow
        pidInit <- forkProcess nd $ initiate coord
        runProcess nd $ do
            res <- liftIO $ flip runReaderT resource $ runExceptT $
                runKatipT le $ runAsyncA (execFlow nd coord store env sciflow) () 
            shutdown coord
            case res of
                Left ex -> error $ show ex
                Right _ -> return ()
            kill pidInit "Exit"

type FlowMonad = KatipT (ExceptT SomeException (ReaderT ResourceConfig IO))

-- | Flow interpreter.
execFlow :: forall coordinator env . (Coordinator coordinator, Binary env)
         => LocalNode
         -> coordinator
         -> CS.ContentStore
         -> env
         -> SciFlow env
         -> AsyncA FlowMonad () ()
execFlow localNode coord store env sciflow = eval (AsyncA . runFlow') $ _flow sciflow
  where
    runFlow' (Step w) = runJob localNode coord store (_function_table sciflow) env w
    runFlow' (UStep f) = \x -> liftIO $ runReaderT (f x) env

runJob :: (Coordinator coordinator, Binary env)
       => LocalNode
       -> coordinator
       -> CS.ContentStore
       -> FunctionTable
       -> env
       -> Job env i o
       -> (i -> FlowMonad o)
runJob localNode coord store rf env Job{..} = runAsyncA $ eval ( \(Action _ key) ->
    AsyncA $ \i -> do
        let chash = key i
            cleanUp ex = do
                CS.removeFailed store chash
                lift $ throwError ex
            input | _job_parallel = encode [i]
                  | otherwise = encode i
            decode' x | _job_parallel = let [r] = decode x in r
                      | otherwise = decode x
        result <- liftIO newEmptyMVar
        -- Block if pending as one node can be executed multiple times
        CS.constructOrWait store chash >>= \case
            CS.Complete item -> liftIO $ decode <$> BS.readFile (simpleOutPath item)
            CS.Missing fp -> handleAny cleanUp $ do
                logMsg mempty InfoS $ ls $ "Running " <> T.unpack _job_name <> ": " <> show chash
                jobRes <- lift $ reader (M.lookup _job_name . _resource_config) >>= \case
                    Nothing -> return _job_resource
                    r -> return r
                liftIO $ runProcess localNode $ do
                    pid <- reserve coord jobRes
                    call (_dict rf) (processNodeId pid)
                        ((_table rf) (_job_name, encode env, input)) >>= \case
                            Just r -> do
                                freeWorker coord pid
                                liftIO $ putMVar result r
                            _ -> error "error"
                liftIO (takeMVar result) >>= writeStore store chash fp . decode'
            _ -> undefined
    ) _job_action
  where
    simpleOutPath item = toFilePath $ CS.itemPath store item </> [relfile|out|]

--------------------------------------------------------------------------------
-- Store functions
--------------------------------------------------------------------------------

writeStore :: (Binary o, MonadIO m)
           => CS.ContentStore -> ContentHash -> Path Abs Dir -> o -> m o
writeStore store chash fp res = do
    liftIO $ BS.writeFile (toFilePath $ fp </> [relfile|out|]) $ encode res
    _ <- CS.markComplete store chash
    return res
{-# INLINE writeStore #-}