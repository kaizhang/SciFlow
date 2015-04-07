{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
module Scientific.Workflow.Builder.TH where

import Language.Haskell.TH

import Control.Monad.State
import qualified Data.Text as T
import qualified Data.HashMap.Strict as M

import Scientific.Workflow.Types
import Scientific.Workflow.Builder

mkWorkflow :: Builder () -> Q Exp
mkWorkflow st = expand s m
  where
    final = execState st []
    s = M.lookupDefault undefined (head $ leaves $ fromUnits $ snd $ unzip final) m
    m = M.fromList final

expand :: Unit -> M.HashMap T.Text Unit -> Q Exp
expand (Link a b) m = [| $(expandA) ~> $(varE b') |]
  where
    a' = mkName $ T.unpack a
    b' = mkName $ T.unpack b
    expandA = case M.lookup a m of
        Nothing -> varE a'
        Just u -> expand u m
expand (Link2 (a,b) c) m = [| m2 $(expandA) $(expandB) ~> $(varE c') |]
  where
    a' = mkName $ T.unpack a
    b' = mkName $ T.unpack b
    c' = mkName $ T.unpack c
    expandA = case M.lookup a m of
        Nothing -> varE a'
        Just u -> expand u m
    expandB = case M.lookup b m of
        Nothing -> varE b'
        Just u -> expand u m
expand _ _ = error "not implemented"
