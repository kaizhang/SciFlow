{-# LANGUAGE CPP           #-}
{-# LANGUAGE DeriveGeneric #-}
module Scientific.Workflow.Internal.Utils
    ( RemoteOpts(..)
    , Log(..)
    , runRemote
    , sendLog
    )where

import qualified Data.ByteString.Char8           as B
import qualified Data.Serialize                  as S
import           Data.Serialize.Text             ()
import qualified Data.Text                       as T
import           Data.Time                       (defaultTimeLocale, formatTime,
                                                  getZonedTime)
import           Data.Yaml                       (ToJSON, encode)
import           GHC.Generics                    (Generic)
import           Network.Socket                  (Socket)
import           Network.Socket.ByteString       (sendAll)
import           Rainbow
import           System.IO

import           Scientific.Workflow.Internal.DB (DBData (..))

#ifdef DRMAA_ENABLED
import           DRMAA                           (DrmaaAttribute (..),
                                                  defaultDrmaaConfig, drmaaRun)
import           System.Environment.Executable   (getExecutablePath)
import           System.IO.Temp                  (withTempDirectory)
#endif

data Log = Running T.Text
         | Complete T.Text
         | Warn T.Text String
         | Error String
         | Exit
         deriving (Generic, Show)

instance S.Serialize Log

getTime :: IO String
getTime = do
    t <- getZonedTime
    return $ formatTime defaultTimeLocale "[%m-%d %H:%M]" t
{-# INLINE getTime #-}

sendLog :: Maybe Socket -> Log -> IO ()
sendLog sock msg = do
    case sock of
        Just s -> sendAll s $ S.encode msg
        _      -> return ()
    case msg of
        Running pid  -> logMsg $ T.unpack pid ++ ": Running..."
        Complete pid -> logMsg $ T.unpack pid ++ ": Finished!"
        Warn pid s   -> warnMsg $ T.unpack pid ++ ": " ++ s
        Error s  -> errorMsg s
        Exit -> return ()

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

data RemoteOpts config = RemoteOpts
    { extraParams :: String
    , environment :: config
    }

runRemote :: (DBData a, DBData b, ToJSON config)
          => RemoteOpts config -> T.Text -> a -> IO b
#ifdef DRMAA_ENABLED
runRemote opts pid input = withTempDirectory tmpDir "drmaa.tmp" $ \dir -> do
    let inputFl = dir ++ "/drmaa_input.tmp"
        outputFl = dir ++ "/drmaa_output.tmp"
        configFl = dir ++ "/drmaa_config.tmp"

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
runRemote = error "DRMAA support was not turned on."
#endif
