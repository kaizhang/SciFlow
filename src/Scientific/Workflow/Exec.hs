{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Scientific.Workflow.Exec (runSciFlow) where

import           Control.Arrow.Async
import           Control.Arrow.Free                          (eval)
import           Control.Exception.Safe                      (SomeException,
                                                              MonadMask,
                                                              bracket,
                                                              onException,
                                                              try)
import           Control.Funflow
import           Control.Funflow.ContentHashable
import qualified Control.Funflow.ContentStore                as CS
import           Control.Monad.IO.Class                      (MonadIO, liftIO)
import Control.Monad.Trans.Control (MonadBaseControl)
import           Control.Monad.Trans.Class                   (lift)
import Control.Concurrent (threadDelay)
import qualified Data.ByteString                             as BS
import           Data.Void
import           Katip
import           Path
import           System.IO                                   (stderr)

import Scientific.Workflow.Types
import Scientific.Workflow.Coordinator

-- | Simple evaulation of a flow
runSciFlow :: ( MonadMask m, MonadIO m, MonadBaseControl IO m
              , Coordinator coordinator)
           => Maybe (Connection coordinator)
           -> CS.ContentStore
           -> SciFlow m a b
           -> a
           -> m (Either SomeException b)
runSciFlow conn store flow input = do
    handleScribe <- liftIO $ mkHandleScribe ColorIfTerminal stderr InfoS V2
    let mkLogEnv = liftIO $ registerScribe "stderr" handleScribe defaultScribeSettings =<<
            initLogEnv "SciFlow" "production"
    bracket mkLogEnv (liftIO . closeScribes) $ \le -> do
        let initialContext = ()
            initialNamespace = "executeLoop"
        runKatipContextT le initialContext initialNamespace $ try $
            runAsyncA (runFlowAsync conn store flow) input

-- | Flow interpreter.
runFlowAsync :: ( MonadMask m, MonadIO m, MonadBaseControl IO m
                , Coordinator coordinator)
             => Maybe (Connection coordinator)
             -> CS.ContentStore
             -> SciFlow m a b
             -> AsyncA (KatipContextT m) a b
runFlowAsync conn store = eval (AsyncA . runFlow')
  where
    --runFlow' :: MonadIO m => Flow' (Job m) a b -> (a -> KatipContextT m b)
    runFlow' (Step props f) = runStep store (cache props) $ return . f
    runFlow' (StepIO props f) = runStep store (cache props) $ liftIO . f
    runFlow' (Wrapped props w) = case conn of
        Nothing -> runStep store (cache props) $ lift . _job_action w
        Just conn' -> lift . runJob conn' store (cache props) w
    runFlow' _ = error "Not implemented"

runStep :: (MonadIO m, MonadMask m)
        => CS.ContentStore
        -> Cacher i o
        -> (i -> KatipContextT m o)
        -> (i -> KatipContextT m o)
runStep _ NoCache f = f
runStep store cache f = \i -> do
    let chash = cacherKey cache undefined i
    checkStore chash >>= \case 
        Right contents -> return contents
        Left fp -> (f i >>= writeStore store cache chash fp) `onException`
            CS.removeFailed store chash
  where
    simpleOutPath item = toFilePath $ CS.itemPath store item </> [relfile|out|]
    checkStore chash = CS.constructOrWait store chash >>= \case
        CS.Pending void -> absurd void
        CS.Complete item -> do
            bs <- liftIO . BS.readFile $ simpleOutPath item
            return . Right . cacherReadValue cache $ bs
        CS.Missing fp -> return $ Left fp

runJob :: (MonadIO m, Coordinator coordinator)
       => Connection coordinator
       -> CS.ContentStore
       -> Cacher i o
       -> Job m i o
       -> (i -> m o)
runJob _ _ NoCache Job{..} = _job_action
runJob conn store cache@(Cache key _ _) Job{..} = \input -> do
    let chash = key undefined input
    host <- liftIO $ getClientId conn
    waitOrRun host chash input
  where
    waitOrRun host chash i = CS.waitUntilComplete store chash >>= \case
        Just item -> liftIO $ cacherReadValue cache <$>
            BS.readFile (simpleOutPath item)
        Nothing -> liftIO (getJobHost conn chash) >>= \case
            Nothing -> liftIO (threadDelay 1000000) >> waitOrRun host chash i
            Just host' -> if host == host'
                then do
                    fp <- CS.markPending store chash
                    _job_action i >>= writeStore store cache chash fp
                else liftIO (threadDelay 1000000) >> waitOrRun host chash i
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