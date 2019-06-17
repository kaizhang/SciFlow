{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Control.Workflow.Main.Command.View (view) where

import Data.Aeson.QQ (aesonQQ)
import Data.Aeson (Value, encode)
import Text.RawString.QQ (r)
import qualified Data.ByteString.Lazy.Char8 as B
import Control.Workflow.Interpreter.Graph
import           Options.Applicative
import Text.Printf (printf)
import Data.List (isSuffixOf)

import Control.Workflow.Main.Types

newtype View = View FilePath

instance IsCommand View where
    runCommand (View output) = writeFile output' . renderCytoscape . mkGraph
      where
        output' | ".html" `isSuffixOf` output = output
                | otherwise = output <> ".html"

view :: Parser Command
view = fmap Command $ View
    <$> strArgument
        ( metavar "workflow.html"
       <> help "File name of the HTML output." ) 

-------------------------------------------------------------------------------
-- Graph
-------------------------------------------------------------------------------

-- | Create Dagre representation.
jsonCytoscape :: Graph -> Value
jsonCytoscape (Graph ns es) = [aesonQQ|
    {
       "nodes": #{map jsonNode ns},
       "edges": #{zipWith jsonEdge [0::Int ..] es}
    } |]
  where
    jsonNode Node{..} =
        [aesonQQ| { "data": {"id": #{_id}} } |]
    jsonEdge i Edge{..} = 
        [aesonQQ| {
            "data": {
                "id": #{i},
                "source": #{_from},
                "target": #{_to}
            }
        } |]

-- | Create HTML rendering for the graph.
renderCytoscape :: Graph -> String
renderCytoscape = printf html . runCytoscape
  where
    html = [r|
        <html>
            <head>
            <style>
                #cy {
                    width: 800px;
                    height: 800px;
                    display: block;
                }
            </style>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/dagre/0.8.4/dagre.min.js"></script>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.6.1/cytoscape.min.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/cytoscape-dagre@2.2.2/cytoscape-dagre.min.js"></script>
            </head>
            <body>
                <div id="cy"></div>
            </body>
            <script>%s</script>
        </html>
        |]

runCytoscape :: Graph -> String
runCytoscape gr = printf js $ B.unpack $ encode $ jsonCytoscape gr
  where
    js = [r|
        (function() {
            var cy = cytoscape({
                container: document.getElementById('cy'),
                elements: %s,
                layout: { name: 'dagre' },
                style: [ {
                    selector: 'node',
                    style: {
                        'background-color': '#666',
                        'label': 'data(id)',
                        'text-valign': 'center',
                        'text-halign': 'center',
                        'shape': 'round-rectangle',
                    } }, {
                    selector: 'edge',
                    style: {
                        'curve-style': 'bezier',
                        'target-arrow-shape': 'triangle',
                        'width': 3,
                        'line-color': '#ddd',
                        'target-arrow-color': '#ddd'
                    } }
                ],
            });
        })(); |]