{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module Scientific.Workflow
    ( module Scientific.Workflow.TH
    , module Scientific.Workflow.Types
    , node
    , nodeM
    --, nodePar
    --, nodeParM
    , (~>)
    , path
    ) where

import qualified Data.Text as T
import qualified Data.HashMap.Strict as M
import Control.Monad.State.Lazy (modify)

import Scientific.Workflow.Types
import Scientific.Workflow.TH

-- | Declare a pure computational step.
node :: ToExpQ fun
     => T.Text   -- ^ Node id
     -> fun      -- ^ Template Haskell expression representing
                 -- functions with type @a -> b@.
     -> Builder ()
node i f = modify $ \wf ->
    wf{ _nodes = M.insertWith undefined i nd $ _nodes wf }
  where
    nd = Node [| return . $(toExpQ f) |]
{-# INLINE node #-}

-- | Declare a pure computational step.
nodeM :: ToExpQ fun
      => T.Text   -- ^ Node id
      -> fun      -- ^ Template Haskell expression representing
                 -- functions with type @a -> b@.
      -> Builder ()
nodeM i f = modify $ \wf ->
    wf{ _nodes = M.insertWith undefined i nd $ _nodes wf }
  where
    nd = Node [| $(toExpQ f) |]
{-# INLINE nodeM #-}

{-
nodePar :: ToExpQ fun
        => T.Text   -- ^ Node id
        -> fun      -- ^ Template Haskell expression representing
                    -- functions with type @a -> b@.
        -> Workflow
        -> Workflow
nodePar i f wf = wf{ _nodes = M.insertWith undefined i nd $ _nodes wf }
  where
    nd = Node [| mapA $ mkJob i $ return . $(toExpQ f) |]

nodeParM :: ToExpQ fun
        => T.Text   -- ^ Node id
        -> fun      -- ^ Template Haskell expression representing
                    -- functions with type @a -> b@.
        -> Workflow
        -> Workflow
nodeParM i f wf = wf{ _nodes = M.insertWith undefined i nd $ _nodes wf }
  where
    nd = Node [| mapA $ mkJob i $(toExpQ f) |]
    -}

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

{-
-- | Add a prefix to IDs of nodes for a given builder, i.e.,
-- @id@ becomes @prefix_id@.
namespace :: T.Text -> Workflow -> Workflow
namespace prefix wf = modify (st <>)
  where
    st = execState (builder >> addPrefix) ([], [])
    addPrefix = modify $ \(nodes, edges) ->
        ( map (\x -> x{_nodePid = prefix <> "_" <> _nodePid x}) nodes
        , map (\x -> x{ _edgeFrom = prefix <> "_" <> _edgeFrom x
                      , _edgeTo = prefix <> "_" <> _edgeTo x }) edges )
-}