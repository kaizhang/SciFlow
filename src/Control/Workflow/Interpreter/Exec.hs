{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}

module Control.Workflow.Interpreter.Exec (execFlow) where

import           Control.Arrow.Async
import           Control.Arrow.Free                          (eval)
import Control.Monad.Reader
import Control.Monad.Except (ExceptT, runExceptT, throwError)
import           Control.Exception.Safe                      (SomeException,
                                                              bracket,
                                                              handleAny)
import           Control.Funflow hiding (Step)
import           Control.Funflow.ContentHashable
import qualified Control.Funflow.ContentStore                as CS
import           Control.Monad.IO.Class                      (MonadIO, liftIO)
import           Control.Monad.Trans.Class                   (lift)
import Control.Concurrent (threadDelay)
import Control.Distributed.Process (processNodeId, getSelfPid, register, call, Process)
import Network.Transport (Transport)
import Control.Distributed.Process.Node
import Control.Distributed.Process.MonadBaseControl ()
import qualified Data.ByteString                             as BS
import GHC.Conc (atomically)
import Data.Binary (Binary(..), encode, decode)
import           Katip
import           Path
import           System.IO                                   (stderr)

import Control.Workflow.Types
import Control.Workflow.Coordinator

-- | Flow interpreter.
execFlow :: forall coordinator env . (Coordinator coordinator, Binary env)
         => coordinator
         -> CS.ContentStore
         -> env
         -> SciFlow env
         -> AsyncA (KatipContextT (ExceptT SomeException Process)) () ()
execFlow coord store env sciflow = eval (AsyncA . runFlow') $ _flow sciflow
  where
    runFlow' (Step w) = lift . runJob coord store (_job_cache w) (_function_table sciflow) env w
    runFlow' (UStep f) = \x -> 
        lift $ lift $ liftIO $ runReaderT (f x) env

runJob :: (Coordinator coordinator, Binary i, Binary o, Binary env)
       => coordinator
       -> CS.ContentStore
       -> Cacher i o
       -> FunctionTable
       -> env
       -> Job env i o
       -> (i -> ExceptT SomeException Process o)
runJob _ _ NoCache _ _ _ _ = error "not implement"
runJob coord store cache@(Cache key _ _) remoteFlow env Job{..} input =
    CS.waitUntilComplete store chash >>= \case
        Just item -> liftIO $ cacherReadValue cache <$>
            BS.readFile (simpleOutPath item)
        Nothing -> handleAny cleanUp $ lift $ do
            fp <- CS.markPending store chash
            -- callLocal (_job_action input) >>= writeStore store cache chash fp
            pid <- reserve coord
            res <- call (_dict remoteFlow) (processNodeId pid) $
                (_table remoteFlow) (_job_name, encode env, encode input)
            case res of
                Nothing -> error "error"
                Just r -> do
                    release coord pid
                    writeStore store cache chash fp $ decode r
  where
    chash = key undefined input
    cleanUp ex = do
        CS.removeFailed store chash
        throwError ex
    simpleOutPath item = toFilePath $ CS.itemPath store item </> [relfile|out|]

--------------------------------------------------------------------------------
-- Store functions
--------------------------------------------------------------------------------

writeStore :: MonadIO m => CS.ContentStore -> Cacher i o -> ContentHash
           -> Path Abs Dir -> o -> m o
writeStore store cache chash fp res = do
    liftIO $ BS.writeFile (toFilePath $ fp </> [relfile|out|]) $
        cacherStoreValue cache $ res
    _ <- CS.markComplete store chash
    return res
{-# INLINE writeStore #-}