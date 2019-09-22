{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Control.Workflow.Main.Command.View (view) where

import Data.Aeson (Value, toJSON)
import Text.RawString.QQ (r)
import Control.Workflow.Interpreter.Graph
import qualified Data.HashMap.Strict as M
import           Options.Applicative
import Data.List (isSuffixOf)
import Language.Javascript.JMacro
import qualified Data.Text as T

import Control.Workflow.Main.Types

newtype View = View FilePath

instance IsCommand View where
    runCommand (View output) = writeFile output' . renderGraph . mkGraph
      where
        output' | ".html" `isSuffixOf` output = output
                | otherwise = output <> ".html"

view :: Parser Command
view = fmap Command $ View
    <$> strArgument
        ( metavar "workflow.html"
       <> help "File name of the HTML output" ) 

-------------------------------------------------------------------------------
-- Graph
-------------------------------------------------------------------------------

-- | Create HTML rendering for the graph.
renderGraph :: Graph -> String
renderGraph gr = html <> runDagre gr <> "</script></html>"
  where
    html = [r|
        <html>
            <head>
            <style>
                html {padding:0px;margin:0px;}
                body {
                    font: 300 14px Helvetica;
                    padding:0px;margin:0px;
                }
                svg {
                    height: 100%;
                    width: 100%;
                }
                .node rect {
                    stroke: #333;
                    fill: #fff;
                }
                .edgePath path {
                    stroke: #333;
                    fill: #333;
                    stroke-width: 1.5px;
                }
                .node text {
                    pointer-events: none;
                }
                .tipsy .description {
                    font-size: 1.2em;
                }
            </style>
            <script src="https://cdn.jsdelivr.net/npm/d3@5.12.0/dist/d3.min.js"></script>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/dagre/0.8.4/dagre.min.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/dagre-d3@0.6.3/dist/dagre-d3.min.js"></script>

            <link rel="stylesheet" href="https://dagrejs.github.io/project/dagre-d3/latest/demo/tipsy.css">
            <script src="https://code.jquery.com/jquery-1.9.1.min.js"></script>
            <script src="https://dagrejs.github.io/project/dagre-d3/latest/demo/tipsy.js"></script>
            </head>
            <body><svg></svg></body><script>
        |]


runDagre :: Graph -> String
runDagre gr = show $ renderJs [jmacro|
    var g = new dagreD3.graphlib.Graph().setGraph({});
    var nodes = `mkNodes gr`;
    var edges = `mkEdges gr`;

    Object.keys(nodes).forEach(function(label) {
        var val = nodes[label];
        val.label = label;
        val.rx = 5;
        val.ry = 5;
        g.setNode(label, val);
    });

    edges.forEach(function(edge) {
        g.setEdge(edge[0], edge[1], {});
    });

    var render = new dagreD3.render();
    var svg = d3.select("svg"), inner = svg.append("g");
    var zoom = d3.zoom().on("zoom", function() {
        inner.attr("transform", d3.event.transform);
    });
    svg.call(zoom);
    var styleTooltip = function(description) {
        return "<p class='description'>" + description + "</p>";
    };
    render(inner, g);
    inner.selectAll("g.node").attr("title", function(v) { return styleTooltip(g.node(v).description) }).each(function(v) { $(this).tipsy({ gravity: "w", opacity: 1, html: true }); });
    //var initialScale = 0.75;
    //svg.call(zoom.transform, d3.zoomIdentity.translate((svg.attr("width") - g.graph().width * initialScale) / 2, 20).scale(initialScale));
    //svg.attr("height", g.graph().height * initialScale + 40);
    |]

mkNodes :: Graph -> Value
mkNodes (Graph ns _) = toJSON $ M.fromList $ flip map ns $
    \Node{..} -> (_label, M.fromList [("description" :: T.Text, _descr)])

mkEdges :: Graph -> Value
mkEdges (Graph _ es) = toJSON $ flip map es $ \Edge{..} -> (_from, _to)