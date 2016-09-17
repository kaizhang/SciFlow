{-# LANGUAGE OverloadedStrings #-}
module Scientific.Workflow.Visualize
    ( renderBuilder
    ) where

import Control.Lens
import Scientific.Workflow.Types (label)
import qualified Data.Text as T
import qualified Data.Text.Lazy      as TL

import qualified Data.GraphViz as G
import qualified Data.GraphViz.Printing as G
import qualified Data.GraphViz.Attributes.Complete as G
import Data.Graph.Inductive.PatriciaTree (Gr)

import Scientific.Workflow.Types

-- | Print the computation graph
renderBuilder :: Gr (PID, Attribute) Int -> TL.Text
renderBuilder dag = G.renderDot . G.toDot $ G.graphToDot param dag
  where
    fmtnode (_, (p, attr)) = [G.Label $ G.StrLabel $ TL.fromStrict lab]
      where
        lab | T.null (attr^.label) = p
            | otherwise = attr^.label
    param = G.nonClusteredParams{G.fmtNode = fmtnode}
