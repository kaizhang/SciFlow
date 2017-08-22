{-# LANGUAGE OverloadedStrings #-}
import Control.Monad
import Network.Socket hiding (recv)
import Network.Socket.ByteString
import Control.Concurrent
import System.IO
import Data.Serialize
import qualified Data.Text as T
import Scientific.Workflow.Internal.Utils
import           System.Process           (CreateProcess (..), ProcessHandle,
                                           StdStream (..), createProcess,
                                           interruptProcessGroupOf, proc)

startSocket = do
    putStrLn "Start socket"
    sock <- socket AF_UNIX Stream defaultProtocol
    let addr = "\0SciFlow.socket"
    bind sock $ SockAddrUnix addr
    listen sock 1
    forever $ do
        (s, _) <- accept sock
        r <- loop s []
        mapM_ putStrLn r
  where
    loop s acc = do
        d <- getData s
        case d of
            Exit -> do
                close s
                return $ "disconnected" : acc
            x -> loop s $ (show x) : acc
    getData s = do
        h <- decode <$> recv s 4096
        case h of
            Right x -> return x
            Left e -> error e

runApp :: IO ()
runApp = do
    (_, _, Just e, p) <- createProcess cmd
        { std_err = CreatePipe
        , new_session = True }
    x <- hGetContents e
    putStrLn x
    return ()
  where
    cmd = proc "./Main" $ ["run", "--log-server", "\\0SciFlow.socket"]

main = do
    forkIO $ startSocket
    --threadDelay 1000000
    runApp
