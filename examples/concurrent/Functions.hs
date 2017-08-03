{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Functions
    (builder) where

import Control.Lens ((^.), (.=), (&))
import qualified Data.Text as T
import Shelly hiding (FilePath)
import Text.Printf (printf)

import Scientific.Workflow

f :: Int -> Int
f = (+1)

-- builder monad
builder :: Builder ()
builder = do
    nodeS "step0" [| return . const [1..10] :: Processor () () [Int] |] $ return ()
    nodeP' 2 "step1" 'f $ label .= "run on remote with batch size 2"
    nodeP' 4 "step2" 'f $ label .= "run on remote with batch size 4"
    node' "step3" [| map f |] $ return ()

    ["step0"] ~> "step1"
    ["step0"] ~> "step2"
    ["step0"] ~> "step3"
