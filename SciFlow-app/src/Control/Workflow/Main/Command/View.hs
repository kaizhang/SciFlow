{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Control.Workflow.Main.Command.View
    ( view
    , renderGraph
    ) where

import Data.Aeson (Value, toJSON)
import qualified Data.HashMap.Strict as M
import           Options.Applicative
import Data.List (isSuffixOf)
import Language.Javascript.JMacro
import qualified Data.Text as T
import qualified Data.Graph.Inductive as G
import Data.Maybe

import Control.Workflow.Main.Types
import Control.Workflow.Types

newtype View = View FilePath

instance IsCommand View where
    runCommand (View output) = writeFile output' . renderGraph . _graph
      where
        output' | ".html" `isSuffixOf` output = output
                | otherwise = output <> ".html"

view :: Parser Command
view = fmap Command $ View
    <$> strArgument
        ( metavar "workflow.html"
       <> help "File name of the HTML output" ) 

mkNodes :: G.Gr (Maybe NodeLabel) () -> [JExpr]
mkNodes gr = flip map (G.labNodes gr) $ \(i, nd) -> case nd of
    Just NodeLabel{..} ->
        [jmacroE| {
            name: `_label`
        }|]
    Nothing -> 
        [jmacroE| {
            name: `show i`,
            label: { show: false}
        }|]

mkEdges :: G.Gr (Maybe NodeLabel) () -> Value
mkEdges gr = toJSON $ flip map (G.edges gr) $ \(fr, to) -> 
    let fr' = maybe (T.pack $ show fr) _label $ fromJust $ G.lab gr fr
        to' = maybe (T.pack $ show to) _label $ fromJust $ G.lab gr to
    in M.fromList [ ("source" :: T.Text, fr'), ("target" :: T.Text, to') ]

--simplyGraph :: G.Gr (Maybe NodeLabel) () -> G.Gr (Maybe NodeLabel) () -> 
--simplyGraph

-------------------------------------------------------------------------------
-- Graph
-------------------------------------------------------------------------------

-- | Create HTML rendering for the graph.
renderGraph :: G.Gr (Maybe NodeLabel) () -> String
renderGraph gr = html
  where
    html = unlines
        [ "<html><head>"
        , "<script src=\"https://cdn.jsdelivr.net/npm/echarts@5.0.2/dist/echarts.min.js\"></script>"
        , "<script src=\"https://cdn.jsdelivr.net/npm/echarts-gl@2.0.2/dist/echarts-gl.min.js\"></script>"
        , "<script src=\"https://cdn.jsdelivr.net/npm/echarts-dagre@0.1.0/dist/echarts-dagre.min.js\"></script>"
        , "</head><body>"
        , "<div id=\"main\" style=\"width:900px; height:900px;\"></div>"
        , "<script type=\"text/javascript\">"
        , runDagre gr
        , "</script></body></html>" ]

runDagre :: G.Gr (Maybe NodeLabel) () -> String
runDagre gr = show $ renderJs [jmacro|
    var myChart = echarts.init(document.getElementById('main'));
    var option = {
        series: [{
            type: 'graph',
            roam: true,
            label: {show: true},
            edgeSymbol: ['circle', 'arrow'],
            layout: 'dagre',
            nodes: `mkNodes gr`,
            links: `mkEdges gr`,
            emphasis: {
                focus: 'adjacency',
                lineStyle: {width: 10}
            }
        }]
    };
    myChart.setOption(option);
    |]