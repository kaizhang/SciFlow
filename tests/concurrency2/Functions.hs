{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Functions
    (builder) where

import Control.Monad
import Control.Lens ((^.), (.=))
import qualified Data.Text as T
import Shelly hiding (FilePath)
import Text.Printf (printf)
import Control.Concurrent

import Scientific.Workflow

--test :: String -> () -> IO ()
test i _ = replicateM_ 2 $ do
    threadDelay 2000000
    putStrLn i

-- builder monad
builder :: Builder ()
builder = do
    node "step0" [| test "0" :: () -> IO () |] $ return ()
    node "step1" [| test "1" :: () -> IO () |] $ return ()
    node "step2" [| test "2" :: () -> IO () |] $ return ()

    ["step0"] ~> "step1"
    ["step0"] ~> "step2"
