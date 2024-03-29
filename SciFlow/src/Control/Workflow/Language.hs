{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module Control.Workflow.Language
    ( -- * Defining workflows
      uNode
    , node
    , nodePar
    , (~>)
    , path
    , namespace
    , Workflow(..)
    , Builder

      -- * Lens for Attributes 
    , doc
    , nCore
    , memory
    , queue
    , Node(..)
    , NodeAttributes
    ) where

import Control.Arrow
import qualified Data.Text as T
import Control.Monad.State.Lazy (State)
import qualified Data.HashMap.Strict as M
import Control.Monad.State.Lazy (modify, execState)
import Data.Maybe (isNothing)
import           Language.Haskell.TH (ExpQ)

import Control.Workflow.Types (Resource(..))

-- | A computation node.
data Node = Node
    { _node_function :: ExpQ  -- ^ a function with type: a -> ReaderT env IO b
    , _node_job_resource :: Maybe Resource   -- ^ Computational resource config
    , _node_parallel :: Bool  -- ^ Should the job be run in parallel
    , _node_doc :: T.Text     -- ^ Documentation
    }
    | UNode ExpQ

data NodeAttributes = NodeAttributes
    { _doc :: T.Text   -- ^ documentation
    , _nCore :: Int
    , _memory :: Int
    , _queue :: Maybe String }

-- | Node description.
doc :: Functor f => (T.Text -> f T.Text) -> NodeAttributes -> f NodeAttributes
doc x y = fmap (\newX -> y { _doc = newX }) (x (_doc y))
{-# INLINE doc #-}

-- | Number of cores.
nCore :: Functor f => (Int -> f Int) -> NodeAttributes -> f NodeAttributes
nCore x y = fmap (\newX -> y { _nCore = newX }) (x (_nCore y))
{-# INLINE nCore #-}

-- | Total memory.
memory :: Functor f => (Int -> f Int) -> NodeAttributes -> f NodeAttributes
memory x y = fmap (\newX -> y { _memory = newX }) (x (_memory y))
{-# INLINE memory #-}

-- | Job queue.
queue :: Functor f
      => (Maybe String -> f (Maybe String))
      -> NodeAttributes -> f NodeAttributes
queue x y = fmap (\newX -> y { _queue = newX }) (x (_queue y))
{-# INLINE queue #-}

mkNode :: ExpQ        -- ^ Template Haskell expression representing
                   -- functions with type @a -> IO b@.
       -> State NodeAttributes ()
       -> Node
mkNode fun attrSetter = Node fun res False $ _doc attr
  where
    res | isNothing core && isNothing mem && isNothing (_queue attr) = Nothing
        | otherwise = Just $ Resource core mem $ _queue attr
    core = if _nCore attr > 1 then Just $ _nCore attr else Nothing
    mem = if _memory attr > 0 then Just $ _memory attr else Nothing
    attr = execState attrSetter $ NodeAttributes
        { _doc = ""
        , _nCore = 1
        , _memory = -1
        , _queue = Nothing }
{-# INLINE mkNode #-}

-- | Workflow declaration, containing a map of nodes and their parental processes.
data Workflow = Workflow
    { _nodes :: M.HashMap T.Text Node
    , _parents :: M.HashMap T.Text [T.Text] }

instance Semigroup Workflow where
    x <> y = Workflow (_nodes x <> _nodes y) (_parents x <> _parents y)

type Builder = State Workflow

-- | Define an uncached step for pure lightweight computation. Such steps will
-- not be saved and will be run each time the workflow is executed.
uNode :: T.Text   -- ^ Node id
      -> ExpQ        -- ^ functions with type @a -> b@.
      -> Builder ()
uNode nid f = modify $ \wf ->
    wf{ _nodes = M.insertWith errMsg nid (UNode f) $ _nodes wf }
  where
    errMsg = error $ "Duplicated nodes: " <> T.unpack nid
{-# INLINE uNode #-}

-- | Define a step.
node :: T.Text   -- ^ Node id
     -> ExpQ        -- ^ Template Haskell expression representing
                 -- functions with type @a -> ReaderT env IO b@.
     -> State NodeAttributes ()   -- ^ Option setter
     -> Builder ()
node nid f attrSetter = modify $ \wf ->
    wf{ _nodes = M.insertWith errMsg nid nd $ _nodes wf }
  where
    nd = mkNode f attrSetter
    errMsg = error $ "Duplicated nodes: " <> T.unpack nid
{-# INLINE node #-}

-- | Define a step that will be executed in parallel, i.e.,
-- @a -> m b@ becomes @[a] -> m [b]@.
nodePar :: T.Text   -- ^ Node id
        -> ExpQ        -- ^ Template Haskell expression representing
                    -- functions with type @a -> ReaderT env IO b@.
        -> State NodeAttributes ()
        -> Builder ()
nodePar i f attrSetter = modify $ \wf ->
    wf{ _nodes = M.insertWith errMsg i nd{_node_parallel=True} $ _nodes wf }
  where
    nd = mkNode f attrSetter
    errMsg = error $ "Duplicated nodes: " <> T.unpack i
{-# INLINE nodePar #-}

linkFromTo :: [T.Text] -> T.Text -> Builder ()
linkFromTo ps to = modify $ \wf ->
    wf{ _parents = M.insertWith errMsg to ps $ _parents wf }
  where
    errMsg = error $ "Duplicated links FROM " <> show ps <> " TO " <> show to
{-# INLINE linkFromTo #-}

-- | Connect nodes.
-- Example:
--
-- > node "step1" [| \() -> return 1 |] $ return ()
-- > node "step2" [| \() -> return 2 |] $ return ()
-- > node "step3" [| \(x, y) -> x * y |] $ return ()
-- > ["step1", "step2"] ~> "step3"
(~>) :: [T.Text] -> T.Text -> Builder ()
(~>) = linkFromTo
{-# INLINE (~>) #-}

-- | @'path' [a, b, c]@ is equivalent to @[a] ~> b >> [b] ~> c@
path :: [T.Text] -> Builder ()
path ns = sequence_ $ zipWith linkFromTo (map return $ init ns) $ tail ns
{-# INLINE path #-}

-- | Add a prefix to IDs of nodes for a given builder, i.e.,
-- @id@ becomes @prefix_id@.
namespace :: T.Text -> Builder () -> Builder ()
namespace prefix builder = modify (st <>)
  where
    st = execState (builder >> addPrefix) $ Workflow M.empty M.empty
    addPrefix = modify $ \Workflow{..} ->
        let nodes = M.fromList $ map (first add) $ M.toList _nodes 
            parents = M.fromList $ map (add *** map add) $ M.toList _parents
        in Workflow nodes parents
    add x = prefix <> "_" <> x
{-# INLINE namespace #-}