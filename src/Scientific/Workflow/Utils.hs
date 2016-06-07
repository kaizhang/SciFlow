{-# LANGUAGE CPP #-}
module Scientific.Workflow.Utils where

import qualified Data.ByteString.Char8         as B
import qualified Data.Text                     as T
import           Debug.Trace                   (traceM)
import           Rainbow
import           System.IO

import           Scientific.Workflow.Types     (DBData (..))
import           System.Directory              (getCurrentDirectory)
import           System.Environment.Executable (getExecutablePath)

#ifdef SGE
import           DRMAA                         (DrmaaAttribute (..),
                                                defaultDrmaaConfig, drmaaRun,
                                                withTmpFile)
#endif

debug :: Monad m => String -> m ()
debug txt = traceM $ B.unpack $ B.concat $
    chunksToByteStrings toByteStringsColors8 [prefix, chunk txt & fore green]
  where
    prefix = bold $ chunk "[DEBUG] " & fore green

error' :: String -> IO ()
error' txt = B.hPutStrLn stderr $ B.concat $
    chunksToByteStrings toByteStringsColors8 [prefix, chunk txt & fore red]
  where
    prefix = bold $ chunk "[ERROR] " & fore red

data RemoteOpts = RemoteOpts
    { extraParams :: String
    }

defaultRemoteOpts :: RemoteOpts
defaultRemoteOpts = RemoteOpts
    { extraParams = ""
    }

runRemote :: (DBData a, DBData b) => RemoteOpts -> T.Text -> a -> IO b
#ifdef SGE
runRemote opts pid input = withTmpFile tmpDir $ \inputFl -> withTmpFile tmpDir $
    \outputFl -> do
        exePath <- getExecutablePath
        wd <- getCurrentDirectory
        let config = defaultDrmaaConfig{drmaa_wd=wd, drmaa_native=extraParams opts}

        B.writeFile inputFl $ serialize input
        drmaaRun exePath ["execFunc", T.unpack pid, inputFl, outputFl] config :: IO ()
        deserialize <$> B.readFile outputFl
  where
    tmpDir = "./"
#else
runRemote = error "SGE support was not turned on."
#endif
