{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module Control.Workflow.Interpreter.Graph
    ( mkGraph
    , Graph(..)
    , Node(..)
    , Edge(..)
    ) where

import           Control.Arrow (Arrow(..))
import Control.Arrow.Free (Free, eval)
import           Control.Category
import qualified Data.Text          as T
import           Prelude            hiding (id, (.))
import qualified Data.HashSet as S
import Data.Hashable (Hashable(..))

import Control.Workflow.Types

mkGraph :: SciFlow env -> Graph
mkGraph flow = Graph ns $ map (\(a,b) -> Edge (_id a) $ _id b) es
  where
    ns = S.toList $ S.fromList $ concatMap (\(a,b) -> [a,b]) es
    es = S.toList $ S.fromList $ toEdges $ toDiagram $ _flow flow
{-# INLINE mkGraph #-}

data Graph = Graph
    { _nodes :: [Node]
    , _edges :: [Edge] }

data Node = Node
    { _id :: T.Text
    , _label :: T.Text
    , _descr :: T.Text }

instance Hashable Node where
    hashWithSalt s = hashWithSalt s . _id

instance Eq Node where
    a == b = _id a == _id b

data Edge = Edge
    { _from :: T.Text
    , _to :: T.Text }

toEdges :: Diagram a b -> [(Node, Node)]
toEdges (Seq f g) = map (\[a,b] -> (a,b)) (sequence [lastD f, headD g]) ++
    toEdges f ++ toEdges g
toEdges (Par f g) = toEdges f ++ toEdges g
toEdges _ = []
{-# INLINE toEdges #-}

headD :: Diagram a b -> [Node]
headD (S nd) = [nd]
headD (Seq Ident g) = headD g
headD (Seq f _) = headD f
headD (Par f g) = headD f ++ headD g
headD _ = []
{-# INLINE headD #-}

lastD :: Diagram a b -> [Node]
lastD (S nd) = [nd]
lastD (Seq f Ident) = lastD f
lastD (Seq _ g) = lastD g
lastD (Par f g) = lastD f ++ lastD g
lastD _ = []
{-# INLINE lastD #-}

toDiagram :: Free (Flow env) a b -> Diagram a b
toDiagram = eval toDiagram'
  where
    toDiagram' (Step Job{..}) = S (Node _job_name _job_name _job_descr)
{-# INLINE toDiagram #-}
  
data Diagram a b where
    Ident :: Diagram a b
    S :: Node -> Diagram a b
    Seq :: Diagram a b -> Diagram b c -> Diagram a c
    Par :: Diagram a b -> Diagram c d -> Diagram (a,c) (b,d)
  
instance Category Diagram where
    id = Ident
    (.) = flip Seq
  
instance Arrow Diagram where
    arr = const Ident
    (***) = Par
  