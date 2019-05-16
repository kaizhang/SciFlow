{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module Control.Workflow.Language
    ( Node(..)
    , Workflow(..)
    , Builder
    , RemoteException
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
import           Language.Haskell.TH (Name)

-- | A computation node.
data Node = Node
    { _node_function :: Name  -- ^ a function with type: a -> Process b
    , _node_parallel :: Bool }

-- | Workflow declaration, containing a map of nodes and their parental processes.
data Workflow = Workflow
    { _nodes :: M.HashMap T.Text Node
    , _parents :: M.HashMap T.Text [T.Text] }

instance Semigroup Workflow where
    x <> y = Workflow (_nodes x <> _nodes y) (_parents x <> _parents y)

type Builder = State Workflow

newtype RemoteException = RemoteException String deriving (Show)

instance Exception RemoteException

-- | Declare a pure computational step.
node :: T.Text   -- ^ Node id
     -> Name     -- ^ Template Haskell expression representing
                 -- functions with type @a -> IO b@.
     -> Builder ()
node i f = modify $ \wf ->
    wf{ _nodes = M.insertWith undefined i nd $ _nodes wf }
  where
    nd = Node f False
{-# INLINE node #-}

nodePar :: T.Text   -- ^ Node id
        -> Name     -- ^ Template Haskell expression representing
                    -- functions with type @a -> IO b@.
        -> Builder ()
nodePar i f = modify $ \wf ->
    wf{ _nodes = M.insertWith undefined i nd $ _nodes wf }
  where
    nd = Node f True

-- | Declare the dependency between nodes.
-- Example:
--
-- > node' "step1" [| \() -> 1 :: Int |] $ return ()
-- > node' "step2" [| \() -> 2 :: Int |] $ return ()
-- > node' "step3" [| \(x, y) -> x * y |] $ return ()
-- > link ["step1", "step2"] "step3"
linkFromTo :: [T.Text] -> T.Text -> Builder ()
linkFromTo ps to = modify $ \wf ->
    wf{ _parents = M.insertWith undefined to ps $ _parents wf }
{-# INLINE linkFromTo #-}

-- | @(~>) = 'link'@.
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