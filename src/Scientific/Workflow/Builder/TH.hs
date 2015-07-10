{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE CPP #-}
module Scientific.Workflow.Builder.TH where

import Language.Haskell.TH

import Control.Applicative ((<$>), (<*>))
import Control.Arrow ((>>>))
import Control.Monad.State
import Data.List (foldl')
import Data.Maybe (mapMaybe)
import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S
import qualified Data.Text as T
import Shelly (lsT, fromText, shelly, test_d)
import Debug.Trace

import Scientific.Workflow.Types
import Scientific.Workflow.Builder

readWorkflowState :: WorkflowConfig -> IO WorkflowState
readWorkflowState config = do
    fls <- shelly $ do
        e <- test_d $ fromText $ T.pack dir
        if e then lsT $ fromText $ T.pack dir
             else return []
    return $ WorkflowState $ M.fromList $
        zip (map (T.unpack . snd . T.breakOnEnd "/") fls) $ repeat True
  where
    dir = _baseDir config ++ "/" ++ _logDir config ++ "/"

mkWorkflow :: String   -- ^ the name of workflow
           -> WorkflowConfig
           -> Builder ()
           -> Q [Dec]
mkWorkflow name config builder = do
    -- query current node running state
    st <- if _overwrite config
        then return $ WorkflowState M.empty
        else runIO $ readWorkflowState config

#ifdef DEBUG
    traceShowM st
#endif

    -- turn on target nodes
--    let st = st'{_nodeStatus = foldl' (\s (k,v) -> M.insert k v s) (_nodeStatus st') $ zip (fst $ unzip nd) $ repeat False}

    let linkFn (es, nodeSet, acc) x
            | fst x `S.member` nodeSet = (e:es, S.difference nodeSet $ S.fromList usedNodes, acc ++ usedNodes)
            | otherwise = (es, nodeSet, acc)
          where
            (e, usedNodes) = linkFrom st table x

        allNodes = map (\(a,b,_) -> (a,b)) $ _nodes builderSt
        targets = filter (not . (`S.member` S.fromList (finished st)) . fst) $
                  case _buildMode config of
                      All -> allNodes
                      Select xs -> filter ((`elem` xs) . fst) allNodes

        (wf, _, used) = foldl' linkFn ([], S.fromList $ fst $ unzip targets, []) $ sort' targets

        config' = config{_state=st}

    nodeDec <- defineNodes $ filter ((`S.member` S.fromList used) . fst) allNodes -- define node functions

    -- construct workflow
    wfDec <- [d| $( varP $ mkName name ) = Workflows config'
                     $( fmap ListE $ sequence $ wf )

             |]

    return $ nodeDec ++ wfDec
  where
    builderSt = execState builder $ B [] []
    sort' nd = reverse $ mapMaybe f $ topSort gr
      where
        nodeSet = S.fromList $ fst $ unzip nd
        f x | x `S.member` nodeSet = Just (x, M.lookupDefault undefined x table)
            | otherwise = Nothing
    gr = fromFactors . snd . unzip . _links $ builderSt
    table = M.fromList $ _links builderSt

defineNodes :: [(String, ExpQ)] -> Q [Dec]
defineNodes nodes = fmap concat $ mapM f nodes
  where
    f (l, fn) = [d| $(varP $ mkName l) = proc l $(fn) |]
{-# INLINE defineNodes #-}

-- | Start linking processors from a given node
linkFrom :: WorkflowState
         -> M.HashMap ID Factor
         -> (ID, Factor)
         -> (Q Exp, [ID])  -- ^ function exp and nodes being used
linkFrom st table (nid,nd)
    | nodeExist nid = ([| Workflow emptySource |], [nid])  -- do nothing
    | otherwise = let (a,b) = runState (go nd) [] in ([| Workflow $(a) |], b)
  where
    expand x | nodeExist x = do e <- go $ S x
                                return [| nullSource >>> $(e) |]
             | otherwise = go $ M.lookupDefault (S x) x table
    nodeExist x = M.lookupDefault False x (_nodeStatus st)

    go :: Factor -> State [ID] ExpQ
    go (S a) = modify (a:) >> return (varE $ mkName a)
    go (L a t) = do
        e1 <- expand a
        e2 <- go $ S t
        return [| $(e1) >>> $(e2) |]
    go (L2 (a,b) t) = do
        e1 <- expand a
        e2 <- expand b
        e3 <- go $ S t
        return [| (,)
              <$> $(e1)
              <*> $(e2)
              >>> $(e3) |]
    {-
    go (L3 (a,b,c) t) = [| (,,)
        <$> $(expand a)
        <*> $(expand b)
        <*> $(expand c)
        >>> $(go $ S t) |]
    go (L4 (a,b,c,d) t) = [| (,,,)
        <$> $(expand a)
        <*> $(expand b)
        <*> $(expand c)
        <*> $(expand d)
        >>> $(go $ S t) |]
    go (L5 (a,b,c,d,f) t) = [| (,,,,)
        <$> $(expand a)
        <*> $(expand b)
        <*> $(expand c)
        <*> $(expand d)
        <*> $(expand f)
        >>> $(go $ S t) |]
    go (L6 (a,b,c,d,f,e) t) = [| (,,,,,)
        <$> $(expand a)
        <*> $(expand b)
        <*> $(expand c)
        <*> $(expand d)
        <*> $(expand f)
        <*> $(expand e)
        >>> $(go $ S t) |]
        -}
{-# INLINE linkFrom #-}
