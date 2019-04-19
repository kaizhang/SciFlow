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
import Control.Monad.Except (ExceptT, throwError)
import           Control.Exception.Safe                      (SomeException,
                                                              handleAny)
import           Control.Funflow.ContentHashable
import qualified Control.Funflow.ContentStore                as CS
import           Control.Monad.IO.Class                      (MonadIO, liftIO)
import           Control.Monad.Trans.Class                   (lift)
import Control.Distributed.Process (processNodeId, call, Process)
import Control.Distributed.Process.MonadBaseControl ()
import qualified Data.ByteString.Lazy                             as BS
import Data.Binary (Binary(..), encode, decode)
import           Katip
import           Path

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
    runFlow' (Step w) = lift . runJob coord store (_function_table sciflow) env w
    runFlow' (UStep f) = \x -> 
        lift $ lift $ liftIO $ runReaderT (f x) env

runJob :: (Coordinator coordinator, Binary env)
       => coordinator
       -> CS.ContentStore
       -> FunctionTable
       -> env
       -> Job env i o
       -> (i -> ExceptT SomeException Process o)
runJob coord store rf env Job{..} = runAsyncA $ eval ( \(Action _ key) ->
    AsyncA $ \i -> do
        let chash = key i
            cleanUp ex = do
                CS.removeFailed store chash
                throwError ex
            input | _job_parallel = encode [i]
                  | otherwise = encode i
            decode' x | _job_parallel = let [r] = decode x in r
                      | otherwise = decode x
        CS.waitUntilComplete store chash >>= \case
            Just item -> liftIO $ decode <$> BS.readFile (simpleOutPath item)
            Nothing -> handleAny cleanUp $ lift $ do
                fp <- CS.markPending store chash
                -- callLocal (_job_action input) >>= writeStore store chash fp
                pid <- reserve coord
                liftIO $ putStrLn $ "Working: " ++ show chash
                call (_dict rf) (processNodeId pid)
                    ((_table rf) (_job_name, encode env, input)) >>= \case
                        Just r -> do
                            release coord pid
                            writeStore store chash fp $ decode' r
                        _ -> error "error"
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