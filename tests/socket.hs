{-# LANGUAGE OverloadedStrings #-}
import Control.Monad
import Network.Socket hiding (recv)
import Network.Socket.ByteString
import Control.Concurrent
import System.IO
import Data.Serialize
import Scientific.Workflow.Internal.Utils

main = do
    sock <- socket AF_UNIX Stream defaultProtocol
    let addr = "\0SciFlow.socket"
    bind sock $ SockAddrUnix addr
    listen sock 1
    forever $ do
        (s, _) <- accept sock
        loop s
  where
    loop s = do
        d <- getData s
        case d of
            Exit -> putStrLn "disconnected" >> close s
            x -> print x >> loop s
    getData s = do
        h <- decode <$> recv s 100
        case h of
            Right x -> return x
            Left e -> error e
