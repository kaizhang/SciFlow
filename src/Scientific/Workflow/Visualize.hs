{-# LANGUAGE OverloadedStrings #-}
module Scientific.Workflow.Visualize
    ( drawWorkflow
    ) where

import           Control.Lens                               ((^.))
import           Data.Graph.Inductive.PatriciaTree          (Gr)
import qualified Data.GraphViz                              as G
import qualified Data.GraphViz.Attributes.Complete          as G
import qualified Data.GraphViz.Attributes.HTML              as H
import qualified Data.GraphViz.Printing                     as G
import qualified Data.Text                                  as T
import qualified Data.Text.Lazy                             as TL

import           Scientific.Workflow.Internal.Builder.Types (Attribute, note)
import           Scientific.Workflow.Types

-- | Output the computation graph in dot code which can be visualize by Graphviz.
drawWorkflow :: Gr (PID, Attribute) Int -> TL.Text
drawWorkflow dag = G.renderDot . G.toDot $ G.graphToDot param dag
  where
    fmtnode (_, (i, attr)) = [G.Label $ G.HtmlLabel label]
      where
        label = H.Table $ H.HTable (Just []) tableAttr $ header : H.HorizontalRule :
            map toLine (wrap 45 $ if T.null (attr^.note) then "Empty" else attr^.note)
        header = H.Cells [H.LabelCell [] $ H.Text
            [ H.Format H.Bold $ [H.Font [H.PointSize 18] [H.Str $ TL.fromStrict i]]
            ]]
        tableAttr = [ H.Border 0
                    , H.CellPadding 0]
    param = G.nonClusteredParams
        { G.globalAttributes =
            [ G.GraphAttrs
                [ -- G.Ratio G.CompressRatio
                -- , G.Size $ G.GSize 7.20472 (Just 9.72441) True
                ]
            , G.NodeAttrs
                [ G.FillColor [G.WC (G.RGBA 190 174 212 100) Nothing]
                , G.Color [G.WC (G.RGBA 190 174 212 0) Nothing]
                , G.Style [G.SItem G.Filled [], G.SItem G.Rounded []]
                , G.Shape G.BoxShape
                , G.FontName "Anonymous Pro, Courier"
                , G.FontSize 16
                ]
            ]
        , G.fmtNode = fmtnode
        }
    toLine x = H.Cells [H.LabelCell [H.Align H.HLeft] $
        H.Text [H.Str $ TL.fromStrict x]]

wrap :: Int -> T.Text -> [T.Text]
wrap limit = concatMap (combine . foldl f (0, [], []) . T.words) . T.lines
  where
    f (count, acc, line) w = if count + T.length w >= limit
        then (0, [], line ++ [T.unwords $ acc ++ [w]])
        else (count + T.length w + 1, acc ++ [w], line)
    combine (_, acc, line) = line ++ [T.unwords acc]
