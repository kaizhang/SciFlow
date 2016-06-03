-- Require SciFlow-0.5.0 installation
{-# LANGUAGE OverloadedStrings #-}
import Control.Monad
import System.Environment
import qualified Data.Text as T
import Scientific.Workflow.DB
import qualified Data.ByteString.Char8 as B
import Shelly

main :: IO ()
main = do
    [oldDB, newDB] <- getArgs

    db <- openDB newDB
    fls <- shelly $ lsT $ fromText $ T.pack oldDB
    forM_ fls $ \fl -> do
        let pid = snd $ T.breakOnEnd "/" fl
        bs <- B.readFile $ T.unpack fl
        saveDataByteString pid bs db
    closeDB db
