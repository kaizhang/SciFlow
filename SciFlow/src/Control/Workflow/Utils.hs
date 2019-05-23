{-# LANGUAGE OverloadedStrings #-}

module Control.Workflow.Utils
    ( infoS
    , warnS
    , errorS
    ) where

import qualified Data.ByteString.Char8           as B
import           Data.Time                       (defaultTimeLocale, formatTime,
                                                 getZonedTime)
import           Rainbow
import           System.IO
import           Control.Monad.IO.Class                      (MonadIO, liftIO)
    
infoS :: MonadIO m => String -> m ()
infoS txt = liftIO $ do
    t <- getTime
    let prefix = bold $ chunk ("[INFO]" ++ t ++ " ") & fore green
        msg = B.concat $ chunksToByteStrings toByteStringsColors8
            [prefix, chunk txt & fore green]
    B.hPutStrLn stderr msg
{-# INLINE infoS #-}
    
errorS :: MonadIO m => String -> m ()
errorS txt = liftIO $ do
    t <- getTime
    let prefix = bold $ chunk ("[ERROR]" ++ t ++ " ") & fore red
        msg = B.concat $ chunksToByteStrings toByteStringsColors8
            [prefix, chunk txt & fore red]
    B.hPutStrLn stderr msg
{-# INLINE errorS #-}
    
warnS :: MonadIO m => String -> m ()
warnS txt = liftIO $ do
    t <- getTime
    let prefix = bold $ chunk ("[WARN]" ++ t ++ " ") & fore yellow
        msg = B.concat $ chunksToByteStrings toByteStringsColors8
            [prefix, chunk txt & fore red]
    B.hPutStrLn stderr msg
{-# INLINE warnS #-}

getTime :: IO String
getTime = formatTime defaultTimeLocale "[%m-%d %H:%M]" <$> getZonedTime
{-# INLINE getTime #-}