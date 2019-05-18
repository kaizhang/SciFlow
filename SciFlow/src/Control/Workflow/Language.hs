{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module Control.Workflow.Language
    ( Node(..)
    , NodeAttributes
    , doc
    , nCore
    , memory
    , queue
    , Workflow(..)
    , Builder
    , node
    , nodePar
    , (~>)
    , path
    , namespace
    ) where

import Control.Arrow
import qualified Data.Text as T
import Control.Exception.Safe (Exception)
import Control.Monad.State.Lazy (State)
import qualified Data.HashMap.Strict as M
import Control.Monad.State.Lazy (modify, execState)
import Control.Lens (makeLenses)
import Data.Maybe (isNothing)
import           Language.Haskell.TH (Name)

import Control.Workflow.Types (Resource(..))

-- | A computation node.
data Node = Node
    { _node_function :: Name  -- ^ a function with type: a -> ReaderT env IO b
    , _node_job_resource :: Maybe Resource   -- ^ Computational resource config
    , _node_parallel :: Bool  -- ^ Should the job be run in parallel
    , _node_doc :: T.Text     -- ^ Documentation
    }

data NodeAttributes = NodeAttributes
    { _doc :: T.Text   -- ^ documentation
    , _nCore :: Int
    , _memory :: Int
    , _queue :: Maybe String }

makeLenses ''NodeAttributes

mkNode :: Name     -- ^ Template Haskell expression representing
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

-- | Declare a pure computational step.
node :: T.Text   -- ^ Node id
     -> Name     -- ^ Template Haskell expression representing
                 -- functions with type @a -> IO b@.
     -> State NodeAttributes ()
     -> Builder ()
node i f attrSetter = modify $ \wf ->
    wf{ _nodes = M.insertWith undefined i nd $ _nodes wf }
  where
    nd = mkNode f attrSetter
{-# INLINE node #-}

nodePar :: T.Text   -- ^ Node id
        -> Name     -- ^ Template Haskell expression representing
                    -- functions with type @a -> IO b@.
        -> State NodeAttributes ()
        -> Builder ()
nodePar i f attrSetter = modify $ \wf ->
    wf{ _nodes = M.insertWith undefined i nd{_node_parallel=True} $ _nodes wf }
  where
    nd = mkNode f attrSetter
{-# INLINE nodePar #-}

linkFromTo :: [T.Text] -> T.Text -> Builder ()
linkFromTo ps to = modify $ \wf ->
    wf{ _parents = M.insertWith undefined to ps $ _parents wf }
{-# INLINE linkFromTo #-}

-- | Declare the dependency between nodes.
-- Example:
--
-- > node' "step1" [| \() -> 1 :: Int |] $ return ()
-- > node' "step2" [| \() -> 2 :: Int |] $ return ()
-- > node' "step3" [| \(x, y) -> x * y |] $ return ()
-- > ["step1", "step2"] ~> "step3"
(~>) :: [T.Text] -> T.Text -> Builder ()
(~>) = linkFromTo
{-# INLINE (~>) #-}

-- | "@'path' [a, b, c]@" is equivalent to "@'link' a b >> 'link' b c@"
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