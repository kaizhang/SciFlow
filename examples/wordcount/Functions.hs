{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Functions
    (builder) where

import Control.Lens ((^.), (.=))
import qualified Data.Text as T
import Shelly hiding (FilePath)
import Text.Printf (printf)

import Scientific.Workflow

create :: () -> IO FilePath
create _ = do
    writeFile "hello.txt" "hello world"
    return "hello.txt"

countWords :: FilePath -> IO Int
countWords fl = do
    content <- readFile fl
    return $ length $ words content

countChars :: FilePath -> IO Int
countChars fl = do
    content <- readFile fl
    return $ sum $ map length $ words content

output :: (Int, Int) -> IO Bool
output (ws, cs) = do
    putStrLn $ printf "Number of words: %d" ws
    putStrLn $ printf "Number of characters: %d" cs
    return True

cleanUp :: (Bool, FilePath) -> IO ()
cleanUp (toBeRemoved, fl) = if toBeRemoved
    then shelly $ rm $ fromText $ T.pack fl
    else return ()

-- builder monad
builder :: Builder ()
builder = do
    node "step0" 'create $ label .= "write something to a file"
    node "step1" 'countWords $ label .= "word count"
    node "step2" 'countChars $ label .= "character count"
    node "step3" 'output $ label .= "print"
    node "step4" 'cleanUp $ label .= "remove the file"

    ["step0"] ~> "step1"
    ["step0"] ~> "step2"
    ["step1", "step2"] ~> "step3"
    ["step3", "step0"] ~> "step4"
