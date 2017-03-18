{-# LANGUAGE CPP #-}
module Scientific.Workflow.Utils
    ( RemoteOpts(..)
    , defaultRemoteOpts
    , runRemote
    , logMsg
    , errorMsg
    , warnMsg
    )where

import qualified Data.ByteString.Char8         as B
import qualified Data.Map                      as M
import qualified Data.Text                     as T
import           Data.Time                     (defaultTimeLocale, formatTime,
                                                getZonedTime)
import           Data.Yaml                     (encode)
import           Rainbow
import           System.IO

import           Scientific.Workflow.Types     (DBData (..))
import           System.Environment.Executable (getExecutablePath)
import           System.IO.Temp                (withTempFile)

#ifdef SGE
import           DRMAA                         (DrmaaAttribute (..),
                                                defaultDrmaaConfig, drmaaRun)
#endif

getTime :: IO String
getTime = do
    t <- getZonedTime
    return $ formatTime defaultTimeLocale "[%m-%d %H:%M]" t
{-# INLINE getTime #-}

logMsg :: String -> IO ()
logMsg txt = do
    t <- getTime
    let prefix = bold $ chunk ("[LOG]" ++ t ++ " ") & fore green
        msg = B.concat $ chunksToByteStrings toByteStringsColors8
            [prefix, chunk txt & fore green]
    B.hPutStrLn stderr msg

errorMsg :: String -> IO ()
errorMsg txt = do
    t <- getTime
    let prefix = bold $ chunk ("[ERROR]" ++ t ++ " ") & fore red
        msg = B.concat $ chunksToByteStrings toByteStringsColors8
            [prefix, chunk txt & fore red]
    B.hPutStrLn stderr msg

warnMsg :: String -> IO ()
warnMsg txt = do
    t <- getTime
    let prefix = bold $ chunk ("[WARN]" ++ t ++ " ") & fore yellow
        msg = B.concat $ chunksToByteStrings toByteStringsColors8
            [prefix, chunk txt & fore red]
    B.hPutStrLn stderr msg

data RemoteOpts = RemoteOpts
    { extraParams :: String
    , environment :: M.Map T.Text T.Text
    }

defaultRemoteOpts :: RemoteOpts
defaultRemoteOpts = RemoteOpts
    { extraParams = ""
    , environment = M.empty
    }

runRemote :: (DBData a, DBData b) => RemoteOpts -> T.Text -> a -> IO b
#ifdef SGE
runRemote opts pid input = withTempFile tmpDir "drmaa.tmp" $ \inputFl _ ->
    withTempFile tmpDir "drmaa.tmp" $ \outputFl _ ->
        withTempFile tmpDir "drmaa.tmp" $ \configFl _ -> do
            B.writeFile configFl $ encode $ environment opts

            exePath <- getExecutablePath
            let config = defaultDrmaaConfig{drmaa_native=extraParams opts}

            B.writeFile inputFl $ serialize input
            drmaaRun exePath [ "execFunc", "--config", configFl, T.unpack pid
                , inputFl, outputFl ] config :: IO ()
            deserialize <$> B.readFile outputFl
  where
    tmpDir = "./"
#else
runRemote = error "SGE support was not turned on."
#endif
