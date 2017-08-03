{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Functions
    (builder) where

import Control.Lens ((^.), (.=))
import qualified Data.Text as T
import Text.Printf (printf)
import Control.Monad.IO.Class (liftIO)

import Scientific.Workflow

f :: Int -> IO ()
f i = error $ show i

f2 :: Int -> ProcState ()
f2 i = do
    x <- getConfig' "text"
    liftIO $ putStrLn x

f3 :: () -> IO Int
f3 _ = return 10

-- builder monad
builder :: Builder ()
builder = do
    node "step0" [| return . const 0 :: () -> IO Int |] $ return ()
    node "step1" [| f2 |] $ return ()
    node "step2" 'f3 $ return ()
    node "step3" 'f3 $ return ()

    path ["step0", "step1", "step2"]
    ["step1"] ~> "step3"
