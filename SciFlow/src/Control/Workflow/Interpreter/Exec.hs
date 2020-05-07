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
import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S
import Network.Transport (Transport)
import Data.Binary (Binary(..), encode, decode)
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import qualified Data.Text as T
import Data.List (foldl')

import Control.Workflow.Types
import Control.Workflow.Utils
import Control.Workflow.Coordinator
import Control.Workflow.DataStore
import Control.Workflow.Interpreter.Graph

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
    nd <- newLocalNode transport $ _rtable $ _function_table sciflow
    pidInit <- forkProcess nd $ initiate coord
    runProcess nd $ do
        let fun = execFlow nd coord store
                (fmap (getDependencies (mkGraph sciflow)) selection)
                sciflow
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
            else const $ throwError EarlyStopped
      where
        run = runJob localNode coord store (_function_table sciflow) job
    runFlow' (UStep fun) = \i -> handleAll cleanUp $ lift $ lift $ fun i
      where
        cleanUp (SomeException ex) = do
            errorS $ "Failed: " <> show ex
            throwError Errored
{-# INLINE execFlow #-}

getDependencies :: Graph -> [T.Text] -> S.HashSet T.Text
getDependencies gr ids = go S.empty ids
  where
    go acc [] = acc 
    go acc xs = go (foldl' (flip S.insert) acc xs) parents
      where
        parents = flip concatMap xs $ \i -> M.lookupDefault [] i gr'
    gr' = M.fromListWith (++) $ map (\e -> (_to e, [_from e])) $ _edges gr
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
                setStatus store chash $ Failed $ show ex
                throwError Errored
            input | _job_parallel = encode [i]
                  | otherwise = encode i
            decode' x | _job_parallel = let [r] = decode x in r
                      | otherwise = decode x
            go = queryStatusPending store chash >>= \case
                Pending -> liftIO (threadDelay 100000) >> go
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
                        -- FIXME: Process hangs if remote process is killed.
                        call (_dict rf) (processNodeId pid)
                            ((_table rf) (_job_name, encode env, input)) >>=
                            liftIO . putMVar result
                        freeWorker coord pid
                    liftIO (takeMVar result) >>= \case
                        Left msg -> error msg
                        Right r -> do
                            let res = decode' r
                            saveItem store chash res
                            infoS $ show chash <> ": Complete!"
                            setStatus store chash $ Complete $ encode res
                            return res
        go
    ) _job_action
{-# INLINE runJob #-}