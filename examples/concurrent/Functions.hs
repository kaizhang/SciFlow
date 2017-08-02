{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Functions
    (builder) where

import Control.Lens ((^.), (.=), (&))
import qualified Data.Text as T
import Shelly hiding (FilePath)
import Text.Printf (printf)

import Scientific.Workflow

f :: Int -> IO Int
f = return . (+1)

-- builder monad
builder :: Builder ()
builder = do
    node "step0" [| return . const [1..10] :: () -> IO [Int] |] $ return ()
    nodeP 2 "step1" 'f $ label .= "run on remote with batch size 2"
    nodeP 4 "step2" 'f $ label .= "run on remote with batch size 4"
    node "step3" [| mapM f |] $ return ()

    ["step0"] ~> "step1"
    ["step0"] ~> "step2"
    ["step0"] ~> "step3"
