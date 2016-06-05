{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Functions
    (builder) where

import Control.Lens ((^.), (.=))
import qualified Data.Text as T
import Shelly hiding (FilePath)
import Text.Printf (printf)

import Scientific.Workflow

dat :: Int
dat = 1

-- builder monad
builder :: Builder ()
builder = do
    node "step0" [| return . const dat :: () -> IO Int |] $ return ()
    node "step1" [| const $ return 2 :: Int -> IO Int |] $ return ()
    node "step2" [| const $ return 2 :: Int -> IO Int |] $ return ()
    node "step3" [| const $ return 2 :: (Int,Int) -> IO Int |] $ return ()
    node "step4" [| const $ return 2 :: (Int,Int) -> IO Int |] $ return ()
    ["step0"] ~> "step1"
    ["step0"] ~> "step2"
    ["step1", "step2"] ~> "step3"
    ["step3", "step0"] ~> "step4"
