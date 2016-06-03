{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Functions
    (builder) where

import Control.Lens ((^.), (.=))
import qualified Data.Text as T
import Shelly hiding (FilePath)
import Text.Printf (printf)

import Scientific.Workflow

f :: Int -> IO Int
f = return . (+1)

-- builder monad
builder :: Builder ()
builder = do
    node "step0" [| return . const 0 |] $ return ()
    node "step1" 'f $ return ()
    node "step2" 'f $ return ()
    node "step3" 'f $ return ()
    node "step4" 'f $ return ()
    node "step5" 'f $ return ()
    node "step6" 'f $ return ()

    ["step0"] ~> "step1"
    ["step0"] ~> "step2"
    ["step1"] ~> "step3"
    ["step1"] ~> "step4"
    ["step2"] ~> "step5"
    ["step2"] ~> "step6"
