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
import           Control.Exception.Safe                      (bracket)
import           System.IO                                   (stderr)
import Network.Transport (Transport)
import Data.Binary (Binary(..), encode, decode)
import           Katip
import           Path
import Control.Concurrent.MVar

import Control.Workflow.Types
import Control.Workflow.Coordinator

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
        pid <- forkProcess nd $ startServer coord
        runProcess nd $ do
            res <- liftIO $ runExceptT $
                runKatipContextT le initialContext initialNamespace $
                runAsyncA (execFlow nd coord store env sciflow) () 
            shutdownServer coord
            case res of
                Left ex -> error $ show ex
                Right _ -> return ()
            kill pid "Exit"

-- | Flow interpreter.
execFlow :: forall coordinator env . (Coordinator coordinator, Binary env)
         => LocalNode
         -> coordinator
         -> CS.ContentStore
         -> env
         -> SciFlow env
         -> AsyncA (KatipContextT (ExceptT SomeException IO)) () ()
execFlow localNode coord store env sciflow = eval (AsyncA . runFlow') $ _flow sciflow
  where
    runFlow' (Step w) = lift . runJob localNode coord store (_function_table sciflow) env w
    runFlow' (UStep f) = \x -> 
        lift $ lift $ liftIO $ runReaderT (f x) env

runJob :: (Coordinator coordinator, Binary env)
       => LocalNode
       -> coordinator
       -> CS.ContentStore
       -> FunctionTable
       -> env
       -> Job env i o
       -> (i -> ExceptT SomeException IO o)
runJob localNode coord store rf env Job{..} = runAsyncA $ eval ( \(Action _ key) ->
    AsyncA $ \i -> do
        let chash = key i
            cleanUp ex = do
                CS.removeFailed store chash
                throwError ex
            input | _job_parallel = encode [i]
                  | otherwise = encode i
            decode' x | _job_parallel = let [r] = decode x in r
                      | otherwise = decode x
        result <- liftIO newEmptyMVar
        CS.waitUntilComplete store chash >>= \case
            Just item -> liftIO $ decode <$> BS.readFile (simpleOutPath item)
            Nothing -> handleAny cleanUp $ lift $ do
                fp <- CS.markPending store chash
                runProcess localNode $ do
                    pid <- reserve coord
                    liftIO $ putStrLn $ "Working: " ++ show chash
                    call (_dict rf) (processNodeId pid)
                        ((_table rf) (_job_name, encode env, input)) >>= \case
                            Just r -> do
                                release coord pid
                                liftIO $ putMVar result r
                            _ -> error "error"
                liftIO (takeMVar result) >>= writeStore store chash fp . decode'
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