{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module Control.Workflow.Interpreter.Visualize
    ( showDiagram
    , toDiagram
    , toGraph
    ) where

import           Control.Arrow (Arrow(..))
import Control.Arrow.Free (Free, eval)
import           Control.Category
import           Data.Proxy         (Proxy (..))
import qualified Data.Text          as T
import           Prelude            hiding (id, (.))
import           Text.PrettyPrint
import qualified Data.HashSet as S

import Control.Workflow.Types
  
newtype NodeProperties = NodeProperties { label :: Maybe T.Text }
  
emptyNodeProperties :: NodeProperties
emptyNodeProperties = NodeProperties Nothing
  
data Diagram a b where
    Ident :: Diagram a b
    Node :: NodeProperties
         -> Proxy a
         -> Proxy b
         -> Diagram a b
    Seq :: Diagram a b -> Diagram b c -> Diagram a c
    Par :: Diagram a b -> Diagram c d -> Diagram (a,c) (b,d)
  
instance Category Diagram where
    id = Ident
    (.) = flip Seq
  
instance Arrow Diagram where
    arr = const Ident
    (***) = Par
  
toDiagram :: Free (Flow env) a b -> Diagram a b
toDiagram = eval toDiagram'
  where
    toDiagram' (Step Job{..}) = Node (NodeProperties (Just _job_name)) Proxy Proxy
    toDiagram' (UStep _) = Node (NodeProperties Nothing) Proxy Proxy

showDiagram :: Diagram a b -> String
showDiagram = render . ppDiagram

ppDiagram :: Diagram a b -> Doc
ppDiagram (Node (NodeProperties (Just lbl)) _ _)    = text . T.unpack $ lbl
ppDiagram (Node (NodeProperties Nothing) _ _)    = "no_cache"
ppDiagram Ident = "id"
ppDiagram (Seq f g) = parens $ ppDiagram f <+> text ">>>" <+> ppDiagram g
ppDiagram (Par f g) = parens $ ppDiagram f <+>  text "***" <+> ppDiagram g
        
toGraph :: Diagram a b -> [(Maybe T.Text, Maybe T.Text)]
toGraph = S.toList . S.fromList . map (\(a,b) -> (label a, label b)) . toPair

toPair :: Diagram a b -> [(NodeProperties, NodeProperties)]
toPair (Seq f g) = map (\[a,b] -> (a,b)) (sequence [lastD f, headD g]) ++
    toPair f ++ toPair g
toPair (Par f g) = toPair f ++ toPair g
toPair _ = []

headD :: Diagram a b -> [NodeProperties]
headD (Node nd _ _) = [nd]
headD (Seq Ident g) = headD g
headD (Seq f g) = headD f
headD (Par f g) = headD f ++ headD g
headD _ = []
{-# INLINE headD #-}

lastD :: Diagram a b -> [NodeProperties]
lastD (Node nd _ _) = [nd]
lastD (Seq f Ident) = lastD f
lastD (Seq f g) = lastD g
lastD (Par f g) = lastD f ++ lastD g
lastD _ = []