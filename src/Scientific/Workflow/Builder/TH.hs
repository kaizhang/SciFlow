{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
module Scientific.Workflow.Builder.TH where

import Language.Haskell.TH

import Control.Applicative ((<$>), (<*>))
import Control.Arrow ((>>>))
import Control.Monad.State
import Data.List (foldl')
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
    st' <- runIO $ readWorkflowState config

    -- turn on target nodes
    let st = st'{_nodeStatus = foldl' (\s (k,v) -> M.insert k v s) (_nodeStatus st') $ zip (fst $ unzip nd) $ repeat False}
        f (nodeSet, acc) x
            | x `S.member` nodeSet = ( S.difference nodeSet (S.fromList $ x : reachable x gr)
                                   , x : acc )
            | otherwise = (nodeSet, acc)

    -- construct workflow
    wfDec <- [d| $( varP $ mkName name ) = Workflow config
                     $( fmap ListE $
                        mapM (linkFrom (_overwrite config) st table) targets )
             |]

    return $ nodeDec ++ wfDec
  where
    builderSt = execState builder $ B [] []
    sortedNodes = topSort gr
    gr = fromFactors . snd . unzip . _links $ builderSt
    table = M.fromList $ _links builderSt
    nd = let allNodes = map (\(a,b,_) -> (a,b)) $ _nodes builderSt
         in case _buildMode config of
             All -> allNodes
             Select xs -> filter (\x -> fst x `elem` xs) allNodes
    targets = map (flip (M.lookupDefault undefined) table) $ snd $
              foldl' f (S.fromList $ fst $ unzip nd, []) $ reverse sortedNodes
      where
        f (nodeSet, acc) x
            | x `S.member` nodeSet = ( S.difference nodeSet (S.fromList $ x : reachable x gr)
                                   , x : acc )
            | otherwise = (nodeSet, acc)

defineNodes :: [(String, ExpQ)] -> Q [Dec]
defineNodes nodes = fmap concat $ mapM f nodes
  where
    f (l, fn) = [d| $(varP $ mkName l) = proc l $(fn) |]
{-# INLINE defineNodes #-}

-- | Start linking processors from a given node
linkFrom :: Bool   -- ^ overwrite or not
         -> WorkflowState
         -> M.HashMap String Factor
         -> Factor
         -> Q Exp
linkFrom overwrite st table nd = go nd
  where
    expand x = if nodeExist x
                   then [| nullSource >>> $(go $ S x) |]
                   else go $ M.lookupDefault (S x) x table
    nodeExist x = not overwrite && M.lookupDefault False x (_nodeStatus st)

    go (S a) = varE $ mkName a
    go (L a t) = [| $(expand a) >>> $(go $ S t) |]
    go (L2 (a,b) t) = [| (,)
        <$> $(expand a)
        <*> $(expand b)
        >>> $(go $ S t) |]
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
{-# INLINE linkFrom #-}
