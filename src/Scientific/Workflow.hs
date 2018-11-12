{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module Scientific.Workflow
    ( module Scientific.Workflow.TH
    , module Scientific.Workflow.Types
    , node
    , (~>)
    , path
    ) where

import qualified Data.Text as T
import Data.List
import qualified Data.HashMap.Strict as M
import Control.Funflow
import Control.Funflow.ContentHashable (contentHash, ContentHashable)
import Control.Monad.Identity (Identity(..))
import Data.Store (Store, encode, decodeEx)

import Scientific.Workflow.Types
import Scientific.Workflow.TH

-- | Declare a pure computational step.
node :: ToExpQ fun
     => T.Text   -- ^ Node id
     -> fun      -- ^ Template Haskell expression representing
                 -- functions with type @a -> b@.
     -> Workflow
     -> Workflow
node i f wf = wf{ _nodes = M.insertWith undefined i nd $ _nodes wf }
  where
    nd = Node [| mkJob i $ return . $(toExpQ f) |]
{-# INLINE node #-}

nodePar :: ToExpQ fun
        => T.Text   -- ^ Node id
        -> fun      -- ^ Template Haskell expression representing
                    -- functions with type @a -> b@.
        -> Workflow
        -> Workflow
nodePar i f wf = wf{ _nodes = M.insertWith undefined i nd $ _nodes wf }
  where
    nd = Node [| mapA $ mkJob i $ return . $(toExpQ f) |]


-- | Declare the dependency between nodes.
-- Example:
--
-- > node' "step1" [| \() -> 1 :: Int |] $ return ()
-- > node' "step2" [| \() -> 2 :: Int |] $ return ()
-- > node' "step3" [| \(x, y) -> x * y |] $ return ()
-- > link ["step1", "step2"] "step3"
linkFromTo :: [T.Text] -> T.Text -> Workflow -> Workflow
linkFromTo ps to wf = wf{ _parents = M.insertWith undefined to ps $ _parents wf }
{-# INLINE linkFromTo #-}

-- | @(~>) = 'link'@.
(~>) :: [T.Text] -> T.Text -> Workflow -> Workflow
(~>) = linkFromTo
{-# INLINE (~>) #-}

-- | "@'path' [a, b, c]@" is equivalent to "@'link' a b >> 'link' b c@"
path :: [T.Text] -> Workflow -> Workflow
path ns wf = foldl' f wf $ zip (init ns) $ tail ns
  where
    f flow (fr, to) = linkFromTo [fr] to flow
{-# INLINE path #-}

{-
-- | Add a prefix to IDs of nodes for a given builder, i.e.,
-- @id@ becomes @prefix_id@.
namespace :: T.Text -> Builder () -> Builder ()
namespace prefix builder = modify (st <>)
  where
    st = execState (builder >> addPrefix) ([], [])
    addPrefix = modify $ \(nodes, edges) ->
        ( map (\x -> x{_nodePid = prefix <> "_" <> _nodePid x}) nodes
        , map (\x -> x{ _edgeFrom = prefix <> "_" <> _edgeFrom x
                      , _edgeTo = prefix <> "_" <> _edgeTo x }) edges )
                      -}

mkJob ::  (Store o, ArrowFlow (Job m) ex arr, ContentHashable Identity i)
      => T.Text -> (i -> m o) -> arr i o
mkJob n f = wrap' prop (Job n (JobConfig Nothing Nothing) f)
  where
    prop = Properties
        { name = Just n
        , cache = cacher
        , mdpolicy = Nothing }
    cacher = Cache
        { cacherKey = \_ i -> runIdentity $ contentHash (n, i)
        , cacherStoreValue = encode
        , cacherReadValue = decodeEx }