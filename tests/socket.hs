{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

import Scientific.Workflow
import Scientific.Workflow.Main

build "wf" [t| SciFlow IO Int () |] $ do
    node "S1" [| (*2) |]
    node "S2" [| (+2) |]
    ["S1", "S2"] ~> "S3"
    node "S3" [| (uncurry (+)) |]
    node "S4" [| (+2) |]
    node "S5" [| (+2) |]
    node "S6" [| (\(x,y,z) -> x + y + z) |]
    ["S3", "S4", "S5"] ~> "S6"

main = do
    x <- defaultMain defaultMainOpts wf 2
    print x