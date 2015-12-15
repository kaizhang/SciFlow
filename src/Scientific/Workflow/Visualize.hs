{-# LANGUAGE OverloadedStrings #-}
module Scientific.Workflow.Visualize
    ( builderToDotString
    ) where

import Control.Lens
import Scientific.Workflow.Types
import           Shelly              (fromText, lsT, shelly, test_f, mkdir_p)
import qualified Data.ByteString     as B
import qualified Data.Text.Lazy      as T

import Data.GraphViz
import Data.GraphViz.Printing
import Data.GraphViz.Attributes.Complete

import Scientific.Workflow.Types (note)
import Scientific.Workflow.Builder

builderToDotString :: Builder () -> String
builderToDotString b = T.unpack $ renderDot . toDot $ graphToDot param dag
  where
    fmtnode (n, l) = [Label $ StrLabel $ T.fromStrict $ snd (snd l) ^. note]
    dag = mkDAG b
    param = nonClusteredParams{fmtNode = fmtnode}
