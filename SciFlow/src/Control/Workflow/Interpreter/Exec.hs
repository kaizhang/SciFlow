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
import Control.Monad.Catch (SomeException(..), handleAll, catchAll)
import Control.Distributed.Process (kill, processNodeId, call)
import Control.Distributed.Process.Node (forkProcess, runProcess, newLocalNode, LocalNode)
import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S
import Network.Transport (Transport)
import qualified Data.Graph.Inductive as G
import Data.Binary (Binary(..), encode, decode)
import Control.Concurrent.MVar
import Data.Maybe
import Data.Hashable (hash)
import qualified Data.Text as T
import Data.List (foldl')

import Control.Workflow.Types
import Control.Workflow.Utils
import Control.Workflow.Coordinator
import Control.Workflow.DataStore

runSciFlow :: (Coordinator coordinator, Binary env)
           => coordinator       -- ^ Coordinator backend
           -> Transport         -- ^ Cloud Haskell transport
           -> DataStore         -- ^ Local cache
           -> ResourceConfig    -- ^ Job resource configuration
           -> Maybe [T.Text]    -- ^ A list of job names. If provided, only
                                -- these nodes and their dependencies will
                                -- be executed.
           -> env               -- ^ Environmental variables
           -> SciFlow env
           -> IO ()
runSciFlow coord transport store resource selection env sciflow = do
    saveEnv store env
    nd <- newLocalNode transport $ _rtable $ _function_table sciflow
    pidInit <- forkProcess nd $ initiate coord
    runProcess nd $ do
        let fun = execFlow nd coord store
                (fmap (getDependencies (_graph sciflow)) selection) sciflow
        res <- liftIO $ flip runReaderT env $ flip runReaderT resource $
            runExceptT $ runAsyncA fun ()
        case res of
            Left Errored -> errorS "Program exits with errors"
            Left EarlyStopped -> infoS "Parts of the program finish successfully"
            Right _ -> infoS "Program finishes successfully"
        shutdown coord
        kill pidInit "Exit"
{-# INLINE runSciFlow #-}

data RunStatus = EarlyStopped | Errored

type FlowMonad env = ExceptT RunStatus (ResourceT (Env env))
type ResourceT m = ReaderT ResourceConfig m

-- | Flow interpreter.
execFlow :: forall coordinator env . (Coordinator coordinator, Binary env)
         => LocalNode
         -> coordinator
         -> DataStore
         -> Maybe (S.HashSet T.Text)
         -> SciFlow env
         -> AsyncA (FlowMonad env) () ()
execFlow localNode coord store selection sciflow = eval (AsyncA . runFlow') $ _flow sciflow
  where
    runFlow' (Step job) = case selection of
        Nothing -> run
        Just s -> if _job_name job `S.member` s
            then run
            --else const $ throwError EarlyStopped
            else const $ return $ error $ show $ _job_name job
      where
        run = runJob localNode coord store (_function_table sciflow) job
    runFlow' (UStep jn fun) = case selection of
        Nothing -> \i -> handleAll cleanUp $ lift $ lift $ fun i
        Just s -> if jn `S.member` s
            then \i -> handleAll cleanUp $ lift $ lift $ fun i
            else const $ return $ error $ show jn
      where
        cleanUp (SomeException ex) = do
            errorS $ "Failed: " <> show ex
            throwError Errored
{-# INLINE execFlow #-}

getDependencies :: G.Gr NodeLabel () -> [T.Text] -> S.HashSet T.Text
getDependencies gr ids = S.map f $ go S.empty $ map hash ids
  where
    f i = _label $ fromJust $ G.lab gr i
    go acc [] = acc 
    go acc xs = go (foldl' (flip S.insert) acc xs) parents
      where
        parents = concatMap (G.pre gr) xs
{-# INLINE getDependencies #-}

runJob :: (Coordinator coordinator, Binary env)
       => LocalNode
       -> coordinator
       -> DataStore
       -> FunctionTable
       -> Job env i o
       -> (i -> FlowMonad env o)
runJob localNode coord store rf Job{..} = runAsyncA $ eval ( \(Action _) ->
    AsyncA $ \i -> do
        let chash = mkKey i _job_name
            cleanUp (SomeException ex) = do
                errorS $ show chash <> " Failed: " <> show ex
                throwError Errored
            input | _job_parallel = encode [i]
                  | otherwise = encode i
            decode' x | _job_parallel = let [r] = decode x in r
                      | otherwise = decode x
            go = queryStatus store chash >>= \case
                Failed _ -> throwError Errored
                Complete dat -> return $ decode dat
                Missing -> handleAll cleanUp $ do
                    -- A Hack, because `runProcess` cannot return value.
                    result <- liftIO newEmptyMVar
                    jobRes <- lift $ reader (M.lookup _job_name . _resource_config) >>= \case
                        Nothing -> return _job_resource
                        r -> return r
                    env <- lift $ lift ask
                    liftIO $ runProcess localNode $ do
                        pid <- reserve coord jobRes
                        infoS $ show chash <> ": Running ..."
                        r <- catchAll ( do
                            r <- call (_dict rf) (processNodeId pid)
                                ((_table rf) (_job_name, encode env, input))
                            freeWorker coord pid
                            return r ) $ \(SomeException ex) -> do
                                setWorkerError coord (show ex) pid
                                return $ Left $ show ex
                        liftIO $ putMVar result r
                    liftIO (takeMVar result) >>= \case
                        Left msg -> error msg
                        Right r -> do
                            let res = decode' r
                            saveItem store chash res
                            infoS $ show chash <> ": Complete!"
                            return res
        go
    ) _job_action
{-# INLINE runJob #-}