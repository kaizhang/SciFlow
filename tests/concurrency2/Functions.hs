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

test :: String -> () -> IO ()
test i _ = replicateM_ 3 $ do
    threadDelay 2000000
    putStrLn i

-- builder monad
builder :: Builder ()
builder = do
    node "step0" [| test "0" |] $ return ()
    node "step1" [| test "1" |] $ return ()
    node "step2" [| test "2" |] $ return ()
    node "step3" [| test "3" |] $ return ()
    node "step4" [| test "4" |] $ return ()
    node "step5" [| test "5" |] $ return ()
    node "step6" [| test "6" |] $ return ()
    node "step7" [| test "7" |] $ return ()
    node "step8" [| test "8" |] $ return ()
    node "step9" [| test "9" |] $ return ()
