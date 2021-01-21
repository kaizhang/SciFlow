{-# LANGUAGE OverloadedStrings #-}

module Control.Workflow.Utils
    ( infoS
    , warnS
    , errorS
    , mkNodeId
    ) where

import qualified Data.ByteString.Char8           as B
import Network.Transport (EndPointAddress(..))
import Control.Distributed.Process (NodeId(..))
import qualified Data.Text as T
import           Data.Time                       (defaultTimeLocale, formatTime,
                                                 getZonedTime)
import           Rainbow
import Data.Function ((&))
import           System.IO
import           Control.Monad.IO.Class                      (MonadIO, liftIO)
    
-- | Pretty print info messages.
infoS :: MonadIO m => String -> m ()
infoS txt = liftIO $ do
    t <- getTime
    let prefix = bold $ chunk ("[INFO]" <> t <> " ") & fore green
        msg = B.concat $ chunksToByteStrings toByteStringsColors8
            [prefix, chunk (T.pack txt) & fore green]
    B.hPutStrLn stderr msg
{-# INLINE infoS #-}
    
-- | Pretty print error messages.
errorS :: MonadIO m => String -> m ()
errorS txt = liftIO $ do
    t <- getTime
    let prefix = bold $ chunk ("[ERROR]" <> t <> " ") & fore red
        msg = B.concat $ chunksToByteStrings toByteStringsColors8
            [prefix, chunk (T.pack txt) & fore red]
    B.hPutStrLn stderr msg
{-# INLINE errorS #-}
    
-- | Pretty print warning messages.
warnS :: MonadIO m => String -> m ()
warnS txt = liftIO $ do
    t <- getTime
    let prefix = bold $ chunk ("[WARN]" <> t <> " ") & fore yellow
        msg = B.concat $ chunksToByteStrings toByteStringsColors8
            [prefix, chunk (T.pack txt) & fore red]
    B.hPutStrLn stderr msg
{-# INLINE warnS #-}

-- | Get current time.
getTime :: IO T.Text
getTime = T.pack . formatTime defaultTimeLocale "[%m-%d %H:%M]" <$> getZonedTime
{-# INLINE getTime #-}

-- | Construct node id given server address and port.
mkNodeId :: String    -- ^ Server address
         -> Int       -- ^ Server port
         -> NodeId
mkNodeId ip port = NodeId $ EndPointAddress $ B.intercalate ":" $
    [B.pack ip, B.pack $ show $ port, "0"]
{-# INLINE mkNodeId #-}