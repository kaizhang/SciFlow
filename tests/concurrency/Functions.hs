{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Functions
    (builder) where

import Control.Lens ((^.), (.=))
import qualified Data.Text as T
import Shelly hiding (FilePath)
import Text.Printf (printf)

import Scientific.Workflow

dat :: [Int]
dat = [1..100000]

-- builder monad
builder :: Builder ()
builder = do
    node "step0" [| return . const dat |] $ return ()
    node "step1" [| return . const dat |] $ return ()
    node "step2" [| return . const dat |] $ return ()
    node "step3" [| return . const dat |] $ return ()
    node "step4" [| return . const dat |] $ return ()
    node "step5" [| return . const dat |] $ return ()
    node "step6" [| return . const dat |] $ return ()
    node "step7" [| return . const dat |] $ return ()
    node "step8" [| return . const dat |] $ return ()
