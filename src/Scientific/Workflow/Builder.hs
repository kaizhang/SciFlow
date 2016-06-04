{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE GADTs #-}

module Scientific.Workflow.Builder
    ( node
    , link
    , (~>)
    , path
    , Builder
    , buildWorkflow
    , buildWorkflowPart
    , getWorkflowState
    , mkDAG
    ) where

import Control.Lens ((^.), (%~), _1, _2, _3, at, (.=))
import Control.Exception (try, displayException)
import Control.Monad.Trans.Except (throwE)
import           Control.Monad.State
import Control.Concurrent.MVar
import qualified Data.Text           as T
import Data.Graph.Inductive.Graph
    ( mkGraph
    , lab
    , labNodes
    , outdeg
    , lpre
    , labnfilter
    , nfilter
    , gmap
    , suc
    , subgraph )
import Data.Graph.Inductive.PatriciaTree (Gr)
import Data.List (sortBy)
import Data.Maybe (fromJust)
import Data.Ord (comparing)
import qualified Data.Map as M
import System.IO.Unsafe (unsafePerformIO)

import           Language.Haskell.TH
import qualified Language.Haskell.TH.Lift as T

import Scientific.Workflow.Types
import Scientific.Workflow.DB
import Scientific.Workflow.Utils (debug)
import Text.Printf (printf)


instance T.Lift T.Text where
  lift t = [| T.pack $(T.lift $ T.unpack t) |]


-- | The order of edges
type EdgeOrd = Int

-- | A computation node
type Node = (PID, (ExpQ, Attribute))

-- | Links between computational nodes
type Edge = (PID, PID, EdgeOrd)

type Builder = State ([Node], [Edge])


-- | Declare a computational node
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
-- execute each individual function in a correct order, named $prefix$_sciflow.
-- Lastly, a function table will be created with the name $prefix$_function_table.
buildWorkflow :: String     -- ^ prefix
              -> Builder ()
              -> Q [Dec]
buildWorkflow prefix b = mkWorkflow prefix $ mkDAG b

-- | Build only a part of the workflow that has not been executed. This is used
-- during development for fast compliation.
buildWorkflowPart :: State RunOpt ()
                  -> String
                  -> Builder ()
                  -> Q [Dec]
buildWorkflowPart setOpt wfName b = do
    st <- runIO $ getWorkflowState $ opt^.dbPath
    mkWorkflow wfName $ trimDAG st $ mkDAG b
  where
    opt = execState setOpt defaultRunOpt

getWorkflowState :: FilePath -> IO WorkflowState
getWorkflowState dir = do
    db <- openDB dir
    ks <- getKeys db
    vs <- replicateM (length ks) $ newMVar Success
    return $ WorkflowState db $ M.fromList $ zip ks vs
{-# INLINE getWorkflowState #-}

-- | Objects that can be converted to ExpQ
class ToExpQ a where
    toExpQ :: a -> ExpQ

instance ToExpQ Name where
    toExpQ = varE

instance ToExpQ ExpQ where
    toExpQ = id

type DAG = Gr Node EdgeOrd

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
trimDAG :: WorkflowState -> DAG -> DAG
trimDAG st dag = gmap revise gr
  where
    revise context@(linkTo, _, lab, linkFrom)
        | done (fst lab) && null linkTo = _3._2._1 %~ e $ context
        | otherwise = context
      where
        e x = [| const undefined >=> $(x) |]
    gr = labnfilter f dag
      where
        f (i, (x,_)) = (not . done) x || any (not . done) children
          where children = map (fst . fromJust . lab dag) $ suc dag i
    done x = case M.lookup x (st^.procStatus) of
        Nothing -> False
        Just v -> unsafePerformIO $ do
            v' <- tryReadMVar v
            case v' of
                Just Success -> return True
                _ -> return False
{-# INLINE trimDAG #-}

data Closure where
    Closure :: (Serializable a, Serializable b, Monad m) => (a -> m b) -> Closure

-- Generate codes from a DAG. This function will create functions defined in
-- the builder. These pieces will be assembled to form a function that will
-- execute each individual function in a correct order.
-- Lastly, a function table will be created with the name $prefix$_function_table.
mkWorkflow :: String   -- prefix
           -> DAG -> Q [Dec]
mkWorkflow workflowName dag = do
    -- write node funcitons
    functions <- concat <$> mapM (\(p, (fn,_)) -> [d|
        $(varP $ mkName $ T.unpack p) = mkProc p $(fn) |]) computeNodes

    -- define workflows
    workflows <-
        [d| $(varP $ mkName workflowName) = ( pids, $(ListE <$> mapM ( \x ->
                [| Workflow $(backTrack x) |] ) sinks) ) |]

    -- function table
    funcTable <-
        [d| $(varP $ mkName functionTableName) = M.fromList $(ListE <$> mapM
                ( \x -> [| (T.unpack x, Closure $(varE $ mkName $ T.unpack x))
                |] ) pids) |]

    return $ functions ++ workflows ++ funcTable
  where
    functionTableName = workflowName ++ "_function_table"
    computeNodes = snd $ unzip $ labNodes dag
    pids = fst $ unzip computeNodes
    sinks = labNodes $ nfilter ((==0) . outdeg dag) dag

    backTrack sink = connect sources sink
      where
        sources = map (\(x,_) -> (x, fromJust $ lab dag x)) $
            sortBy (comparing snd) $ lpre dag $ fst sink

    connect [] sink = mkNodeVar sink
    connect [source] sink = [| $(backTrack source) >=> $(mkNodeVar sink) |]
    connect sources sink = [| $(foldl g e0 sources) >=> $(mkNodeVar sink) |]
      where
        e0 = [| (return . return) $(conE (tupleDataName $ length sources)) |]
        g acc x = [| (ap . fmap ap) $(acc) $(backTrack x) |]
    mkNodeVar = varE . mkName . T.unpack . fst . snd
{-# INLINE mkWorkflow #-}

mkProc :: Serializable b => PID -> (a -> IO b) -> (Processor a b)
mkProc pid f = \input -> do
    wfState <- get
    let pSt = M.findWithDefault (error "Impossible") pid $ wfState^.procStatus
#ifdef DEBUG
    isEmpty <- liftIO $ isEmptyMVar pSt
    when isEmpty $ debug $ printf "%s: waiting for other nodes to finish." pid
#endif
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
#ifdef DEBUG
            debug $ printf "Running node: %s" pid
#endif
            result <- liftIO $ try $ f input
            case result of
                Left ex -> liftIO (putMVar pSt $ Fail ex) >> lift (throwE (pid, ex))
                Right r -> liftIO $ do
                    saveData pid r $ wfState^.db
                    putMVar pSt Success
                    return r
{-# INLINE mkProc #-}



--------------------------------------------------------------------------------

{-
newtype Parallel a = Parallel { runParallel :: ProcState a}

instance Functor Parallel where
  fmap f (Parallel a) = Parallel $ f <$> a

instance Applicative Parallel where
  pure = Parallel . return
  Parallel fs <*> Parallel as = Concurrently $ (\(f, a) -> f a) <$> concurrently fs as
  -}
