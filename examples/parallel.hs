{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

import           Control.Lens             ((.=))
import           Scientific.Workflow

f :: Int -> Int
f = (+1)

defaultMain $ namespace "Parallel" $ do
    nodeS "step0" [| return . const [1..10] :: () -> WorkflowConfig () [Int] |] $ return ()
    nodeP' 2 "step1" 'f $ label .= "run in parallel with batch size 2"
    nodeP' 4 "step2" 'f $ label .= "run in parallel with batch size 4"
    node' "step3" [| map f |] $ return ()

    ["step0"] ~> "step1"
    ["step0"] ~> "step2"
    ["step0"] ~> "step3"
