{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}
module Functions
    (builder) where

import           Control.Lens           ((.=), (^.))
import           Control.Monad.IO.Class (liftIO)
import qualified Data.Text              as T
import           Shelly                 hiding (FilePath)
import           Text.Printf            (printf)

import           Scientific.Workflow
import           Scientific.Workflow.Internal.Builder
import           Scientific.Workflow.Types

create :: Processor () () FilePath
create () = liftIO $ do
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

cleanUp :: (Bool, FilePath) -> IO String
cleanUp (toBeRemoved, fl) = do
    when toBeRemoved $ shelly (rm $ fromText $ T.pack fl)
    return "done"

-- builder monad
builder :: Builder ()
builder = do
    nodeS "step0" 'create $ label .= "write something to a file"
    node "step1" 'countWords $ label .= "word count"
    node "step2" 'countChars $ label .= "character count"
    node "step3" 'output $ label .= "print"
    node "step4" 'cleanUp $ label .= "remove the file"

    ["step0"] ~> "step1"
    ["step0"] ~> "step2"
    ["step1", "step2"] ~> "step3"
    ["step3", "step0"] ~> "step4"
