{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

module Scientific.Workflow.Builder
    ( node
    , link
    , (~>)
    , path
    , Builder
    , buildWorkflow
    ) where

import Control.Lens ((^.), (%~), _1, _2, _3)
import           Control.Monad.State
import qualified Data.Text           as T
import Data.Graph.Inductive.Graph (mkGraph, lab, labNodes, outdeg, inn)
import Data.Graph.Inductive.PatriciaTree (Gr)
import Data.List (sortBy)
import qualified Data.Map as M
import Data.Maybe (fromJust)
import Data.Ord (comparing)

import           Language.Haskell.TH
import qualified Language.Haskell.TH.Lift as T

import Scientific.Workflow.Types
import Scientific.Workflow.DB

instance T.Lift T.Text where
  lift t = [| T.pack $(T.lift $ T.unpack t) |]


type EdgeOrd = Int
type Node = (PID, (ExpQ, Attribute))
type Edge = (PID, PID, EdgeOrd)

type Builder = State ([Node], [Edge])

-- | Declare a computational node
node :: PID -> ExpQ -> State Attribute () -> Builder ()
node p fn setAttr = modify $ _1 %~ (newNode:)
  where
    attr = execState setAttr defaultAttribute
    defaultAttribute = Attribute ""
    newNode = (p, (fn, attr))
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

buildWorkflow :: String
              -> Builder ()
              -> Q [Dec]
buildWorkflow wfName b = mkWorkflow wfName $ mkDAG b

-- | Objects that can be converted to ExpQ
class ToExpQ a where
    toExpQ :: a -> ExpQ

instance ToExpQ Name where
    toExpQ = varE

instance ToExpQ ExpQ where
    toExpQ = id

type DAG = Gr Node EdgeOrd

mkDAG :: Builder () -> DAG
mkDAG b = mkGraph ns' es'
  where
    ns' = map (\x -> (pid2nid $ fst x, x)) ns
    es' = map (\(fr, t, o) -> (pid2nid fr, pid2nid t, o)) es
    (ns, es) = execState b ([], [])
    pid2nid p = M.findWithDefault (error "mkDAG") p m
      where
        m = M.fromListWithKey err $ zip (map fst ns) [0..]
        err k _ _ = error $ "multiple instances for: " ++ T.unpack k
{-# INLINE mkDAG #-}

{-
trimDAG :: WorkflowState -> DAG -> DAG
trimDAG st dag =
-}

mkWorkflow :: String -> DAG -> Q [Dec]
mkWorkflow wfName dag = do
    decNode <- concat <$> mapM (f . snd) ns
    decWf <- [d| $(varP $ mkName wfName) = $(fmap ListE $ mapM linking leafNodes)
             |]
    return $ decNode ++ decWf
  where
    f (p, (fn, _)) = [d| $(varP $ mkName $ T.unpack p) = mkProc p $(fn) |]
    ns = labNodes dag
    leafNodes = filter ((==0) . outdeg dag . fst) ns
    linking nd = [| Workflow $(go nd) |]
      where
        go n = connect inNodes n
          where
            inNodes = map (\(x,_,_) -> (x, fromJust $ lab dag x)) $
                      sortBy (comparing (^._3)) $ inn dag $ fst n
        define n = varE $ mkName (T.unpack $ (snd n) ^. _1)
        connect [] t = define t
        connect [s1] t = [| $(go s1) >=> $(define t) |]
        connect [s1,s2] t = [| ( (,) <$$> $(go s1) <**> $(go s2) ) >=> $(define t) |]
        connect xs t = error $ show $ length xs
{-# INLINE mkWorkflow #-}

(<$$>) :: (Functor f1, Functor f2) => (a -> b) -> f1 (f2 a) -> f1 (f2 b)
(<$$>) = fmap . fmap
{-# INLINE (<$$>) #-}

(<**>) :: (Applicative f1, Applicative f2, Functor f1, Functor f2) => f1 (f2 (a -> b)) -> f1 (f2 a) -> f1 (f2 b)
(<**>) = (<*>) . fmap (<*>)
{-# INLINE (<**>) #-}

mkProc :: Serializable b => PID -> (a -> IO b) -> (Processor a b)
mkProc p f = \input -> do
    st <- get
    case M.findWithDefault Scheduled p (_procStatus st) of
        Finished -> lift $ readData p $ _db st
        Scheduled -> lift $ do
            result <- f input
            saveData p result $ _db st
            return result
