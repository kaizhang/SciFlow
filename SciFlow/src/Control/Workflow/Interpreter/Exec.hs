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
import Control.Monad.Catch (SomeException(..), handleAll)
import           Control.Monad.IO.Class                      (liftIO)
import           Control.Monad.Trans (lift)
import Control.Distributed.Process (kill, processNodeId, call)
import Control.Distributed.Process.Node (forkProcess, runProcess, newLocalNode, LocalNode)
import Control.Distributed.Process.MonadBaseControl ()
import qualified Data.HashMap.Strict as M
import Network.Transport (Transport)
import Data.Binary (Binary(..), encode, decode)
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar

import Control.Workflow.Types
import Control.Workflow.Utils
import Control.Workflow.Coordinator
import Control.Workflow.DataStore

runSciFlow :: (Coordinator coordinator, Binary env)
           => coordinator       -- ^ Coordinator backend
           -> Transport         -- ^ Cloud Haskell transport
           -> DataStore         -- ^ Local cache
           -> ResourceConfig    -- ^ Job resource configuration
           -> env               -- ^ Optional environmental variables
           -> SciFlow env
           -> IO ()
runSciFlow coord transport store resource env sciflow = do
    nd <- newLocalNode transport $ _rtable $ _function_table sciflow
    pidInit <- forkProcess nd $ initiate coord
    runProcess nd $ do
        res <- liftIO $ flip runReaderT resource $ runExceptT $
            runAsyncA (execFlow nd coord store env sciflow) () 
        shutdown coord
        case res of
            Left ex -> errorS "Program exit with errors"
            Right _ -> infoS "Program finish successfully"
        kill pidInit "Exit"
{-# INLINE runSciFlow #-}

type FlowMonad = ExceptT String (ReaderT ResourceConfig IO)

-- | Flow interpreter.
execFlow :: forall coordinator env . (Coordinator coordinator, Binary env)
         => LocalNode
         -> coordinator
         -> DataStore
         -> env
         -> SciFlow env
         -> AsyncA FlowMonad () ()
execFlow localNode coord store env sciflow = eval (AsyncA . runFlow') $ _flow sciflow
  where
    runFlow' (Step w) = runJob localNode coord store (_function_table sciflow) env w
{-# INLINE execFlow #-}

runJob :: (Coordinator coordinator, Binary env)
       => LocalNode
       -> coordinator
       -> DataStore
       -> FunctionTable
       -> env
       -> Job env i o
       -> (i -> FlowMonad o)
runJob localNode coord store rf env Job{..} = runAsyncA $ eval ( \(Action _) ->
    AsyncA $ \i -> do
        let chash = mkKey i _job_name
            cleanUp (SomeException ex) = do
                throwError $ show ex
            input | _job_parallel = encode [i]
                  | otherwise = encode i
            decode' x | _job_parallel = let [r] = decode x in r
                      | otherwise = decode x
            go = queryStatus store chash >>= \case
                Just Pending -> liftIO (threadDelay 1000) >> go
                Just (Failed msg) -> throwError msg
                Just Complete -> fetchItem store chash
                Nothing -> handleAll cleanUp $ do
                    -- A Hack, because `runProcess` cannot return value.
                    result <- liftIO newEmptyMVar
                    infoS $ show chash <> ": Running ..."
                    jobRes <- lift $ reader (M.lookup _job_name . _resource_config) >>= \case
                        Nothing -> return _job_resource
                        r -> return r
                    liftIO $ runProcess localNode $ do
                        pid <- reserve coord jobRes
                        call (_dict rf) (processNodeId pid)
                            ((_table rf) (_job_name, encode env, input)) >>=
                            liftIO . putMVar result
                        freeWorker coord pid
                    liftIO (takeMVar result) >>= \case
                        Left msg -> do
                            errorS $ show chash <> " Failed: " <> msg
                            throwError msg
                        Right r -> do
                            let res = decode' r
                            saveItem store chash res
                            infoS $ show chash <> ": Complete!"
                            return res
        go
    ) _job_action
{-# INLINE runJob #-}