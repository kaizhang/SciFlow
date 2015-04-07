{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
module Scientific.Workflow.Builder.TH where

import Language.Haskell.TH

import Control.Arrow ((>>>))
import Control.Monad.State
import qualified Data.Text as T
import qualified Data.HashMap.Strict as M

import Control.Arrow (Kleisli)
import Control.Monad.Reader (ReaderT, lift, reader, (>=>))
import Scientific.Workflow.Types
import Scientific.Workflow.Serialization
import Scientific.Workflow.Builder

mkWorkflow :: Builder () -> Q Exp
mkWorkflow st = expand s m
  where
    builder = execState st $ B [] []
    s = M.lookupDefault undefined (head $ leaves $ fromUnits $ snd $ unzip $ _links builder) m
    m = M.fromList $ _links builder
    nd = map (\(l,_,_) -> l) $ _nodes builder

mkNodesTH :: Builder () -> Q [Dec]
mkNodesTH st = do d <- mapM f nodes
                  return $ concat d
  where
    f (l, ar) = [d| $(varP $ mkName l) = proc l $(varE $ mkName ar) |]
    nodes = map (\(a,b,_) -> (a,b)) $ _nodes $ execState st $ B [] []

expand :: Unit -> M.HashMap String Unit -> Q Exp
expand (Link a b) m = [| $(expandA) >>> $(varE b') |]
  where
    a' = mkName a
    b' = mkName b
    expandA = case M.lookup a m of
        Nothing -> varE a'
        Just u -> expand u m
expand (Link2 (a,b) c) m = [| m2 $(expandA) $(expandB) >>> $(varE c') |]
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

