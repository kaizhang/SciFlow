{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards #-}

module Scientific.Workflow.Internal.Builder
    ( node
    , node'
    , nodeS
    , nodeP
    , nodeP'
    , nodePS
    , nodeSharedP
    , nodeSharedP'
    , nodeSharedPS
    , link
    , (~>)
    , path
    , namespace
    , buildWorkflow
    , buildWorkflowPart
    , mkDAG
    , mkProc
    ) where

import Control.Monad.Identity (runIdentity)
import Data.Monoid ((<>))
import Control.Lens ((^.), (%~), _1, _2, _3, (&))
import Control.Monad.Trans.Except (throwE)
import Control.Monad.State (lift, liftIO, (>=>), foldM_, execState, modify, State)
import Control.Monad.Reader (ask)
import Control.Concurrent.MVar
import Control.Concurrent (forkIO)
import qualified Data.Text           as T
import Data.List.Split (chunksOf)
import Data.Yaml (ToJSON)
import Data.Graph.Inductive.Graph ( mkGraph, lab, labNodes, outdeg, nmap
                                  , lpre, labnfilter, nfilter, gmap, suc )
import Data.List (sortBy, foldl')
import Data.Maybe (fromJust, fromMaybe)
import qualified Data.ByteString as B
import Data.Ord (comparing)
import qualified Data.Map as M
import Control.Concurrent.Async.Lifted (mapConcurrently)
import           Language.Haskell.TH
import Control.Monad.Catch (try)

import Scientific.Workflow.Types
import Scientific.Workflow.Internal.Builder.Types
import Scientific.Workflow.Internal.DB
import Scientific.Workflow.Internal.Utils (sendLog, Log(..), runRemote, RemoteOpts(..))

nodeWith :: ToExpQ q
         => FunctionConfig
         -> PID                  -- ^ node id
         -> q                    -- ^ function
         -> State Attribute ()   -- ^ Attribues
         -> Builder ()
nodeWith conf pid fn setAttr = modify $ _1 %~ (newNode:)
  where
    attr = execState setAttr defaultAttribute{_functionConfig = conf}
    newNode = Node pid (toExpQ fn) attr
{-# INLINE nodeWith #-}

-- | Declare a computational node.
-- Input Function: (DBData a, DBData b) => a -> m b, where m = IO or ProcState
-- Result: a -> ProcState b
node :: ToExpQ q
     => PID                  -- ^ node id
     -> q                    -- ^ function
     -> State Attribute ()   -- ^ Attribues
     -> Builder ()
node = nodeWith $ FunctionConfig None IOAction
{-# INLINE node #-}

node' :: ToExpQ q => PID -> q -> State Attribute () -> Builder ()
node' = nodeWith $ FunctionConfig None Pure
{-# INLINE node' #-}

nodeS :: ToExpQ q => PID -> q -> State Attribute () -> Builder ()
nodeS = nodeWith $ FunctionConfig None Stateful
{-# INLINE nodeS #-}

-- | Declare a computational node. The function must have the signature:
-- Input Function: (DBData a, DBData b) => a -> m b
-- Result: [a] -> m [b]
nodeP :: ToExpQ q => Int -> PID -> q -> State Attribute () -> Builder ()
nodeP n = nodeWith $ FunctionConfig (Standard n) IOAction
{-# INLINE nodeP #-}

nodeP' :: ToExpQ q => Int -> PID -> q -> State Attribute () -> Builder ()
nodeP' n = nodeWith $ FunctionConfig (Standard n) Pure
{-# INLINE nodeP' #-}

nodePS :: ToExpQ q => Int -> PID -> q -> State Attribute () -> Builder ()
nodePS n = nodeWith $ FunctionConfig (Standard n) Stateful
{-# INLINE nodePS #-}

nodeSharedP :: ToExpQ q => Int -> PID -> q -> State Attribute () -> Builder ()
nodeSharedP n = nodeWith $ FunctionConfig (ShareData n) IOAction
{-# INLINE nodeSharedP #-}

nodeSharedP' :: ToExpQ q => Int -> PID -> q -> State Attribute () -> Builder ()
nodeSharedP' n = nodeWith $ FunctionConfig (ShareData n) Pure
{-# INLINE nodeSharedP' #-}

nodeSharedPS :: ToExpQ q => Int -> PID -> q -> State Attribute () -> Builder ()
nodeSharedPS n = nodeWith $ FunctionConfig (ShareData n) Stateful
{-# INLINE nodeSharedPS #-}

-- | Many-to-one generalized link function
link :: [PID] -> PID -> Builder ()
link xs t = modify $ _2 %~ (zipWith3 Edge xs (repeat t) [0..] ++)
{-# INLINE link #-}

-- | (~>) = link.
(~>) :: [PID] -> PID -> Builder ()
(~>) = link
{-# INLINE (~>) #-}

path :: [PID] -> Builder ()
path ns = foldM_ f (head ns) $ tail ns
  where
    f a t = link [a] t >> return t
{-# INLINE path #-}

-- | Add a prefix to IDs of nodes for a given builder, i.e.,
-- @id@ becomes @prefix_id@.
namespace :: T.Text -> Builder () -> Builder ()
namespace prefix builder = builder >> addPrefix
  where
    addPrefix = modify $ \(nodes, edges) ->
        ( map (\x -> x{_nodePid = prefix <> "_" <> _nodePid x}) nodes
        , map (\x -> x{ _edgeFrom = prefix <> "_" <> _edgeFrom x
                      , _edgeTo = prefix <> "_" <> _edgeTo x }) edges )

-- | Build the workflow. This function will first create functions defined in
-- the builder. These pieces will then be assembled to form a function that will
-- execute each individual function in a correct order, named $name$.
buildWorkflow :: String     -- ^ Name of the workflow
              -> Builder () -- ^ Builder
              -> Q [Dec]
buildWorkflow workflowName = mkWorkflow workflowName . mkDAG

-- | Build only a part of the workflow that has not been executed. This is used
-- during development for fast compliation.
buildWorkflowPart :: FilePath   -- ^ Path to the db
                  -> String     -- ^ Name of the workflow
                  -> Builder () -- ^ Builder
                  -> Q [Dec]
buildWorkflowPart dbPath wfName b = do
    st <- runIO $ getWorkflowState dbPath
    mkWorkflow wfName $ trimDAG st $ mkDAG b
  where
    getWorkflowState fl = do
        db <- openDB fl
        ks <- getKeys db
        return $ M.fromList $ zip ks $ repeat Success

-- TODO: check the graph is a valid DAG
-- | Contruct a DAG representing the workflow
mkDAG :: Builder () -> DAG
mkDAG builder = mkGraph ns' es'
  where
    ns' = map (\x -> (pid2nid $ _nodePid x, x)) ns
    es' = map (\Edge{..} -> (pid2nid _edgeFrom, pid2nid _edgeTo, _edgeOrd)) es
    (ns, es) = execState builder ([], [])
    pid2nid pid = M.findWithDefault
        (error $ "mkDAG: cannot identify node: " ++ T.unpack pid) pid $
        M.fromListWithKey
            (\k _ _ -> error $ "Multiple declaration for: " ++ T.unpack k) $
            zip (map _nodePid ns) [0..]
{-# INLINE mkDAG #-}

-- | Remove nodes that are executed before from a DAG.
trimDAG :: (M.Map T.Text NodeState) -> DAG -> DAG
trimDAG st dag = gmap revise gr
  where
    revise context@(linkTo, _, nodeLabel, _)
        | shallBuild (_nodePid nodeLabel) && null linkTo = context
        | otherwise = context & _3 %~
            ( \l -> l{_nodeFunction = feedEmptyInput (_nodeFunction l)} )
      where
        feedEmptyInput x = [| (\() -> undefined) >=> $(x) |]
    gr = labnfilter f dag
      where
        f (i, x) = shallBuild (_nodePid x) || any shallBuild children
          where children = map (_nodePid . fromJust . lab dag) $ suc dag i
    shallBuild x = case M.lookup x st of
        Just Success -> False
        _ -> True
{-# INLINE trimDAG #-}


-- Generate codes from a DAG. This function will create functions defined in
-- the builder. These pieces will be assembled to form a function that will
-- execute each individual function in a correct order.
-- Lastly, a function table will be created with the name $name$_function_table.
mkWorkflow :: String   -- name
           -> DAG -> Q [Dec]
mkWorkflow workflowName dag =
    [d| $(varP $ mkName workflowName) = Workflow dag' pids $workflowMain |]
  where
    workflowMain = connect sinks [| const $ return () |]
    dag' = nmap _nodePid dag
    computeNodes = snd $ unzip $ labNodes dag
    pids = M.fromList $ map (\Node{..} -> (_nodePid, _nodeAttr)) computeNodes
    sinks = labNodes $ nfilter ((==0) . outdeg dag) dag

    backTrack (i, Node{..}) = connect (fst $ unzip parents) [| $mkP $fun |]
      where
        parents = map ( \(x, o) -> ((x, fromJust $ lab dag x), o) ) $
            sortBy (comparing snd) $ lpre dag i
        fun = case _nodeAttr^.functionConfig of
            FunctionConfig _ Pure -> [| return . $_nodeFunction |]
            FunctionConfig _ IOAction -> [| liftIO . $_nodeFunction |]
            FunctionConfig _ Stateful -> [| (lift . lift) . $_nodeFunction |]
        mkP = case _nodeAttr^.functionConfig of
            FunctionConfig None _ -> [| mkProc _nodePid |]
            FunctionConfig (Standard n) _ -> [| mkProcListN n _nodePid |]
            FunctionConfig (ShareData n) _ -> [| mkProcListNWithContext n _nodePid |]

    connect [] sink = sink
    connect [source] sink = [| $(backTrack source) >=> $sink |]
    connect sources sink = [| fmap runParallel $expq >=> $sink |]
      where
        expq = foldl' g e0 $ sources
        e0 = [| (pure. pure) $(conE (tupleDataName $ length sources)) |]
        g acc x = [| ((<*>) . fmap (<*>)) $acc $ fmap Parallel $(backTrack x) |]
{-# INLINE mkWorkflow #-}

mkProc :: (DBData a, DBData b, ToJSON config)
       => PID -> (a -> (ProcState config) b) -> (Processor config a b)
mkProc = mkProcWith (return, runIdentity)
{-# INLINE mkProc #-}

mkProcListN :: (DBData [a], DBData [b], ToJSON config)
            => Int
            -> PID
            -> (a -> (ProcState config) b)
            -> (Processor config [a] [b])
mkProcListN n pid f = mkProcWith (chunksOf n, concat) pid $ mapM f
{-# INLINE mkProcListN #-}

mkProcListNWithContext :: (DBData (ContextData c [a]), DBData [b], ToJSON config)
                       => Int -> PID
                       -> (ContextData c a -> (ProcState config) b)
                       -> (Processor config (ContextData c [a]) [b])
mkProcListNWithContext n pid f = mkProcWith (toChunks, concat) pid f'
  where
    f' (ContextData c xs) = mapM f $ zipWith ContextData (repeat c) xs
    toChunks (ContextData c xs) = zipWith ContextData (repeat c) $ chunksOf n xs
{-# INLINE mkProcListNWithContext #-}

mkProcWith :: (Traversable t, DBData a, DBData b, ToJSON config)
           => (a -> t a, t b -> b) -> PID
           -> (a -> (ProcState config) b)
           -> (Processor config a b)
mkProcWith (box, unbox) pid f = \input -> do
    wfState <- ask
    let (pSt, attr) = M.findWithDefault (error "Impossible") pid $ wfState^.procStatus

    pStValue <- liftIO $ takeMVar pSt
    case pStValue of
        (Fail ex) -> liftIO (putMVar pSt pStValue) >> lift (throwE (pid, ex))
        Success -> liftIO $ do
            putMVar pSt pStValue
            readData pid $ wfState^.database
        Scheduled -> do
            _ <- liftIO $ takeMVar $ wfState^.procParaControl

            liftIO $ sendLog (wfState^.logServer) $ Running pid

            config <- lift $ lift ask
            let sendToRemote = fromMaybe (wfState^.remote) (attr^.submitToRemote)
                remoteOpts = RemoteOpts
                    { extraParams = attr^.remoteParam
                    , environment = config
                    }
                input' = box input
            result <- try $ unbox <$> if sendToRemote
                then liftIO $ mapConcurrently (runRemote remoteOpts pid) input'
                else mapM f input'  -- disable parallel in local machine due to memory issue
            case result of
                Left ex -> do
                    _ <- liftIO $ do
                        putMVar pSt $ Fail ex
                        _ <- forkIO $ putMVar (wfState^.procParaControl) ()
                        sendLog (wfState^.logServer) $ Warn pid "Failed!"
                    lift (throwE (pid, ex))
                Right r -> liftIO $ do
                    saveData pid r $ wfState^.database
                    putMVar pSt Success
                    _ <- forkIO $ putMVar (wfState^.procParaControl) ()
                    sendLog (wfState^.logServer) $ Complete pid
                    return r

        Special mode -> handleSpecialMode mode wfState pSt pid f
{-# INLINE mkProcWith #-}

handleSpecialMode :: (DBData a, DBData b)
                  => SpecialMode
                  -> WorkflowState
                  -> MVar NodeState -> PID
                  -> (a -> (ProcState config) b)
                  -> (ProcState config) b
handleSpecialMode mode wfState nodeSt pid fn = case mode of
    Skip -> liftIO $ putMVar nodeSt (Special Skip) >> return undefined

    EXE inputData output -> do
        c <- liftIO $ B.readFile inputData
        r <- fn $ deserialize c
        liftIO $ B.writeFile output $ serialize r
        liftIO $ putMVar nodeSt $ Special Skip
        return r

    -- Read data stored in this node
    FetchData -> liftIO $ do
        r <- readData pid $ wfState^.database
        B.putStr $ showYaml r
        putMVar nodeSt $ Special Skip
        return r

    -- Replace data stored in this node
    WriteData inputData -> do
        c <- liftIO $ B.readFile inputData
        r <- return (readYaml c) `asTypeOf` fn undefined
        liftIO $ do
            updateData pid r $ wfState^.database
            putMVar nodeSt $ Special Skip
            return r
{-# INLINE handleSpecialMode #-}
