{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
module Scientific.Workflow.Builder.TH where

import Language.Haskell.TH

import Control.Arrow ((>>>))
import Control.Monad.State
import qualified Data.HashMap.Strict as M

import Scientific.Workflow.Types
import Scientific.Workflow.Builder

mkWorkflow :: String -> Builder () -> Q [Dec]
mkWorkflow name st = do
    nodeDec <- declareNodesTH nd
    wfDec <- [d| $(varP $ mkName name) = $(fmap ListE $ mapM (`expand'` m) endNodes) |]
    return $ nodeDec ++ wfDec
  where
    builder = execState st $ B [] []
    endNodes = map (\x -> M.lookupDefault undefined x m) . leaves . fromUnits . snd . unzip . _links $ builder
    m = M.fromList $ _links builder
    nd = map (\(a,b,_) -> (a,b)) $ _nodes builder

declareNodesTH :: [(String, String)] -> Q [Dec]
declareNodesTH nodes = do d <- mapM f nodes
                          return $ concat d
  where
    f (l, ar) = [d| $(varP $ mkName l) = proc l $(varE $ mkName ar) |]

expand' :: Unit -> M.HashMap String Unit -> Q Exp
expand' l m = [| Workflow $(expand l m) |]

expand :: Unit -> M.HashMap String Unit -> Q Exp
expand (Link a b) m = [| $(expandA) >>> $(varE b') |]
  where
    a' = mkName a
    b' = mkName b
    expandA = case M.lookup a m of
        Nothing -> varE a'
        Just u -> expand u m
expand (Link2 (a,b) c) m = [| zipS $(expandA) $(expandB) >>> $(varE c') |]
  where
    a' = mkName a
    b' = mkName b
    c' = mkName c
    expandA = case M.lookup a m of
        Nothing -> varE a'
        Just u -> expand u m
    expandB = case M.lookup b m of
        Nothing -> varE b'
        Just u -> expand u m
expand _ _ = error "not implemented"

