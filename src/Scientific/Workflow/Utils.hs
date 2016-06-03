module Scientific.Workflow.Utils where

import Debug.Trace (traceM)
import qualified Data.ByteString.Char8 as B
import System.IO
import Rainbow

debug :: Applicative f => String -> f ()
debug txt = traceM $ B.unpack $ B.concat $
    chunksToByteStrings toByteStringsColors8 [prefix, chunk txt & fore green]
  where
    prefix = bold $ chunk "[DEBUG] " & fore green

error' :: String -> IO ()
error' txt = B.hPutStrLn stderr $ B.concat $
    chunksToByteStrings toByteStringsColors8 [prefix, chunk txt & fore red]
  where
    prefix = bold $ chunk "[ERROR] " & fore red
