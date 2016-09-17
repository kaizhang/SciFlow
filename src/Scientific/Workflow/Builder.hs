{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE CPP #-}

module Scientific.Workflow.Builder
    ( node
    , link
    , (~>)
    , path
    , buildWorkflow
    , buildWorkflowPart
    , mkDAG
    ) where

import Control.Lens ((^.), (%~), _1, _2, _3)
import Control.Monad.Trans.Except (throwE)
import           Control.Monad.State (lift, liftIO, (>=>), foldM_, execState, modify, State, get)
import Control.Concurrent.MVar
import Control.Concurrent (forkIO)
import qualified Data.Text           as T
import Data.Graph.Inductive.Graph ( mkGraph, lab, labNodes, outdeg
                                  , lpre, labnfilter, nfilter, gmap, suc )
import Data.Graph.Inductive.PatriciaTree (Gr)
import Data.List (sortBy, foldl')
import Data.Maybe (fromJust, fromMaybe)
import qualified Data.ByteString as B
import Data.Ord (comparing)
import qualified Data.Map as M
import Text.Printf (printf)
import Control.Concurrent.Async.Lifted (mapConcurrently)
import           Language.Haskell.TH
import Control.Monad.Catch (try)

import Scientific.Workflow.Types
import Scientific.Workflow.DB
import Scientific.Workflow.Utils (debug, runRemote, defaultRemoteOpts, RemoteOpts(..))


-- | Declare a computational node. The function must have the signature:
-- (DBData a, DBData b) => a -> IO b
node :: ToExpQ q
     => PID                  -- ^ node id
     -> q                    -- ^ function
     -> State Attribute ()   -- ^ Attribues
     -> Builder ()
node p fn setAttr = modify $ _1 %~ (newNode:)
  where
    attr = execState setAttr defaultAttribute
    newNode = (p, (toExpQ fn, attr))
{-# INLINE node #-}


{-
 Declare a function that can be called on remote
function :: ToExpQ q => T.Text -> q -> Builder ()
function funcName fn =
-}


-- | many-to-one generalized link function
link :: [PID] -> PID -> Builder ()
link xs t = modify $ _2 %~ (zip3 xs (repeat t) [0..] ++)
{-# INLINE link #-}

-- | (~>) = link.
(~>) :: [PID] -> PID -> Builder ()
(~>) = link
{-# INLINE (~>) #-}

-- | singleton
path :: [PID] -> Builder ()
path ns = foldM_ f (head ns) $ tail ns
  where
    f a t = link [a] t >> return t
{-# INLINE path #-}

-- | Build the workflow. This function will first create functions defined in
-- the builder. These pieces will then be assembled to form a function that will
-- execute each individual function in a correct order, named $name$.
-- Lastly, a function table will be created with the name $name$_function_table.
buildWorkflow :: String     -- ^ name
              -> Builder ()
              -> Q [Dec]
buildWorkflow prefix b = mkWorkflow prefix $ mkDAG b

-- | Build only a part of the workflow that has not been executed. This is used
-- during development for fast compliation.
buildWorkflowPart :: FilePath   -- ^ path to the db
                  -> String
                  -> Builder ()
                  -> Q [Dec]
buildWorkflowPart db wfName b = do
    st <- runIO $ getWorkflowState db
    mkWorkflow wfName $ trimDAG st $ mkDAG b
  where
    getWorkflowState dir = do
        db <- openDB dir
        ks <- getKeys db
        return $ M.fromList $ zip ks $ repeat Success

-- | Objects that can be converted to ExpQ
class ToExpQ a where
    toExpQ :: a -> ExpQ

instance ToExpQ Name where
    toExpQ = varE

instance ToExpQ ExpQ where
    toExpQ = id

type DAG = Gr Node EdgeOrd

-- TODO: check the graph is a valid DAG
-- | Contruct a DAG representing the workflow
mkDAG :: Builder () -> DAG
mkDAG b = mkGraph ns' es'
  where
    ns' = map (\x -> (pid2nid $ fst x, x)) ns
    es' = map (\(fr, t, o) -> (pid2nid fr, pid2nid t, o)) es
    (ns, es) = execState b ([], [])
    pid2nid p = M.findWithDefault errMsg p m
      where
        m = M.fromListWithKey err $ zip (map fst ns) [0..]
        err k _ _ = error $ "multiple instances for: " ++ T.unpack k
        errMsg = error $ "mkDAG: cannot identify node: " ++ T.unpack p
{-# INLINE mkDAG #-}

-- | Remove nodes that are executed before from a DAG.
trimDAG :: (M.Map T.Text NodeResult) -> DAG -> DAG
trimDAG st dag = gmap revise gr
  where
    revise context@(linkTo, _, lab, _)
        | done (fst lab) && null linkTo = _3._2._1 %~ e $ context
        | otherwise = context
      where
        e x = [| (\() -> undefined) >=> $(x) |]
    gr = labnfilter f dag
      where
        f (i, (x,_)) = (not . done) x || any (not . done) children
          where children = map (fst . fromJust . lab dag) $ suc dag i
    done x = case M.lookup x st of
        Just Success -> True
        _ -> False
{-# INLINE trimDAG #-}


-- Generate codes from a DAG. This function will create functions defined in
-- the builder. These pieces will be assembled to form a function that will
-- execute each individual function in a correct order.
-- Lastly, a function table will be created with the name $name$_function_table.
mkWorkflow :: String   -- name
           -> DAG -> Q [Dec]
mkWorkflow workflowName dag = do
    let expq = connect sinks [| const $ return () |]
    -- define the workflow
    workflows <-
        [d| $(varP $ mkName workflowName) = Workflow pids $expq |]

    return workflows
  where
    computeNodes = snd $ unzip $ labNodes dag
    pids = M.fromList $ map (\(i, x) -> (i, snd x)) computeNodes
    sinks = labNodes $ nfilter ((==0) . outdeg dag) dag

    backTrack (i, (p, (fn, attr)))
        | attr^.stateful = connect (fst $ unzip parents) [| mkProc p $fn |]
        | otherwise = connect (fst $ unzip parents) [| mkProc p (liftIO . $fn) |]
      where
        parents = map ( \(x, o) -> ((x, fromJust $ lab dag x), o) ) $
            sortBy (comparing snd) $ lpre dag i

    connect [] sink = sink
    connect [source] sink = [| $expq >=> $sink |]
      where
        expq = backTrack source
    connect sources sink = [| fmap runParallel $expq >=> $sink |]
      where
        expq = foldl' g e0 $ sources
        e0 = [| (pure. pure) $(conE (tupleDataName $ length sources)) |]
        g acc x =
            let expq = backTrack x
            in [| ((<*>) . fmap (<*>)) $acc $ fmap Parallel $expq |]
{-# INLINE mkWorkflow #-}

mkProc :: (BatchData' (IsList a b) a b, BatchData a b, DBData a, DBData b)
       => PID -> (a -> ProcState b) -> (Processor a b)
mkProc pid f = \input -> do
    wfState <- get
    let (pSt, attr) = M.findWithDefault (error "Impossible") pid $ wfState^.procStatus

    pStValue <- liftIO $ takeMVar pSt
    case pStValue of
        (Fail ex) -> liftIO (putMVar pSt pStValue) >> lift (throwE (pid, ex))
        Success -> liftIO $ do
            putMVar pSt pStValue

#ifdef DEBUG
            debug $ printf "Recovering saved node: %s" pid
#endif

            readData pid $ wfState^.db
        Scheduled -> do
            _ <- liftIO $ takeMVar $ wfState^.procParaControl

#ifdef DEBUG
            debug $ printf "Running node: %s" pid
#endif

            let sendToRemote = fromMaybe (wfState^.remote) (attr^.submitToRemote)
                remoteOpts = defaultRemoteOpts{environment=wfState^.config}
            result <- try $ case () of
                _ | attr^.batch > 0 -> do
                    let (mkBatch, combineResult) = batchFunction f $ attr^.batch
                        input' = mkBatch input
                    combineResult <$> if sendToRemote
                        then liftIO $ mapConcurrently (runRemote remoteOpts pid) input'
                        else mapM f input'
                  | otherwise -> if sendToRemote
                      then liftIO $ runRemote remoteOpts pid input
                      else f input
            case result of
                Left ex -> do
                    _ <- liftIO $ do
                        putMVar pSt $ Fail ex
                        forkIO $ putMVar (wfState^.procParaControl) ()
                    lift (throwE (pid, ex))
                Right r -> liftIO $ do
                    saveData pid r $ wfState^.db
                    putMVar pSt Success
                    _ <- forkIO $ putMVar (wfState^.procParaControl) ()
                    return r
        Skip -> liftIO $ putMVar pSt pStValue >> return undefined
        EXE input output -> do
            c <- liftIO $ B.readFile input
            r <- f $ deserialize c
            liftIO $ B.writeFile output $ serialize r
            liftIO $ putMVar pSt Skip
            return undefined
        Read -> liftIO $ do
            r <- readData pid $ wfState^.db
            B.putStr $ showYaml r
            putMVar pSt Skip
            return r
        Replace input -> do
            c <- liftIO $ B.readFile input
            r <- return (readYaml c) `asTypeOf` f undefined
            liftIO $ updateData pid r $ wfState^.db
            liftIO $ putMVar pSt Skip
            return r
{-# INLINE mkProc #-}
