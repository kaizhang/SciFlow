{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
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
    nodeDec <- defineNodes nd   -- ^ define node functions

    -- query current node running state
    st <- runIO $ readWorkflowState config

    -- turn on target nodes
--    let st = st'{_nodeStatus = foldl' (\s (k,v) -> M.insert k v s) (_nodeStatus st') $ zip (fst $ unzip nd) $ repeat False}

    let linkFn (es, nodeSet) x
            | fst x `S.member` nodeSet = (e:es, S.difference nodeSet $ S.fromList usedNodes)
            | otherwise = (es, nodeSet)
          where
            (e, usedNodes) = linkFrom (_overwrite config) st table x

    -- construct workflow
    wfDec <- [d| $( varP $ mkName name ) = Workflows config
                     $( fmap ListE $ sequence $ fst $
                        foldl' linkFn ([], S.fromList $ fst $ unzip nd) sortedNodes )
             |]

    return $ nodeDec ++ wfDec
  where
    builderSt = execState builder $ B [] []
    sortedNodes = reverse $ mapMaybe f $ topSort gr
      where
        nodeSet = S.fromList nids
        f x | x `S.member` nodeSet = Just (x, M.lookupDefault undefined x table)
            | otherwise = Nothing
    gr = fromFactors . snd . unzip . _links $ builderSt
    table = M.fromList $ _links builderSt
    nids = fst $ unzip nd
    nd = let allNodes = map (\(a,b,_) -> (a,b)) $ _nodes builderSt
         in case _buildMode config of
             All -> allNodes
             Select xs -> filter (\x -> fst x `elem` xs) allNodes

defineNodes :: [(String, ExpQ)] -> Q [Dec]
defineNodes nodes = fmap concat $ mapM f nodes
  where
    f (l, fn) = [d| $(varP $ mkName l) = proc l $(fn) |]
{-# INLINE defineNodes #-}

-- | Start linking processors from a given node
linkFrom :: Bool   -- ^ overwrite or not
         -> WorkflowState
         -> M.HashMap ID Factor
         -> (ID, Factor)
         -> (Q Exp, [ID])  -- ^ function exp and nodes being used
linkFrom overwrite st table (nid,nd)
    | nodeExist nid = ([| Workflow emptySource |], [nid])  -- do nothing
    | otherwise = let (a,b) = runState (go nd) [] in ([| Workflow $(a) |], b)
  where
    expand x | nodeExist x = do e <- go $ S x
                                return [| nullSource >>> $(e) |]
             | otherwise = go $ M.lookupDefault (S x) x table
    nodeExist x = not overwrite && M.lookupDefault False x (_nodeStatus st)

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
