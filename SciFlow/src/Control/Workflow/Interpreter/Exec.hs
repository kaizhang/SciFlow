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
                env sciflow
        res <- liftIO $ runReaderT (runExceptT $ runAsyncA fun ()) resource
        case res of
            Left Errored -> errorS "Program exits with errors"
            Left EarlyStopped -> infoS "Parts of the program finish successfully"
            Right _ -> infoS "Program finishes successfully"
        shutdown coord
        kill pidInit "Exit"
{-# INLINE runSciFlow #-}

data RunStatus = EarlyStopped | Errored

type FlowMonad = ExceptT RunStatus (ReaderT ResourceConfig IO)

-- | Flow interpreter.
execFlow :: forall coordinator env . (Coordinator coordinator, Binary env)
         => LocalNode
         -> coordinator
         -> DataStore
         -> Maybe (S.HashSet T.Text)
         -> env
         -> SciFlow env
         -> AsyncA FlowMonad () ()
execFlow localNode coord store selection env sciflow = eval (AsyncA . runFlow') $ _flow sciflow
  where
    runFlow' (Step job) = case selection of
        Nothing -> run
        Just s -> if _job_name job `S.member` s
            then run
            else const $ throwError EarlyStopped
      where
        run = runJob localNode coord store (_function_table sciflow) env job
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
       -> env
       -> Job env i o
       -> (i -> FlowMonad o)
runJob localNode coord store rf env Job{..} = runAsyncA $ eval ( \(Action _) ->
    AsyncA $ \i -> do
        let chash = mkKey i _job_name
            cleanUp (SomeException ex) = do
                setStatus store chash $ Failed $ show ex
                throwError Errored
            input | _job_parallel = encode [i]
                  | otherwise = encode i
            decode' x | _job_parallel = let [r] = decode x in r
                      | otherwise = decode x
            go = queryStatusPending store chash >>= \case
                Just Pending -> liftIO (threadDelay 100000) >> go
                Just (Failed _) -> throwError Errored
                Just Complete -> fetchItem store chash
                Nothing -> handleAll cleanUp $ do
                    -- A Hack, because `runProcess` cannot return value.
                    result <- liftIO newEmptyMVar
                    jobRes <- lift $ reader (M.lookup _job_name . _resource_config) >>= \case
                        Nothing -> return _job_resource
                        r -> return r
                    liftIO $ runProcess localNode $ do
                        pid <- reserve coord jobRes
                        infoS $ show chash <> ": Running ..."
                        call (_dict rf) (processNodeId pid)
                            ((_table rf) (_job_name, encode env, input)) >>=
                            liftIO . putMVar result
                        freeWorker coord pid
                    liftIO (takeMVar result) >>= \case
                        Left msg -> do
                            errorS $ show chash <> " Failed: " <> msg
                            setStatus store chash $ Failed msg
                            throwError Errored
                        Right r -> do
                            let res = decode' r
                            saveItem store chash res
                            infoS $ show chash <> ": Complete!"
                            setStatus store chash Complete
                            return res
        go
    ) _job_action
{-# INLINE runJob #-}