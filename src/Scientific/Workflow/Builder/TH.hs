{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
module Scientific.Workflow.Builder.TH where

import Language.Haskell.TH

import Control.Applicative ((<$>), (<*>))
import Control.Arrow ((>>>))
import Control.Monad.State
import Data.Default.Class
import qualified Data.HashMap.Strict as M

import Scientific.Workflow.Types
import Scientific.Workflow.Utils (fileExist)
import Scientific.Workflow.Builder


mkWorkflow :: String   -- ^ the name of workflow
           -> Builder ()
           -> Q [Dec]
mkWorkflow name builder = do
--    st <- runIO $ readWorkflowState config $ fst $ unzip nd

    nodeDec <- defineNodes nd   -- ^ define node functions

    -- construct workflow
    wfDec <- [d| $( varP $ mkName name ) = $( fmap ListE $ mapM (linkFrom table) leafNodes )
             |]

    return $ nodeDec ++ wfDec
  where
    builderSt = execState builder $ B [] []
    leafNodes = map (flip (M.lookupDefault undefined) table) . leaves .
                fromFactors . snd . unzip . _links $ builderSt
    table = M.fromList $ _links builderSt
    nd = map (\(a,b,_) -> (a,b)) $ _nodes builderSt

defineNodes :: [(String, ExpQ)] -> Q [Dec]
defineNodes nodes = fmap concat $ mapM f nodes
  where
    f (l, fn) = [d| $(varP $ mkName l) = proc l $(fn) |]
{-# INLINE defineNodes #-}

-- | Start linking processors from a given node
linkFrom :: M.HashMap String Factor
         -> Factor
         -> Q Exp
linkFrom table nd = [| Workflow $(go nd) |]
  where
    expand x = go $ M.lookupDefault (S x) x table

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
