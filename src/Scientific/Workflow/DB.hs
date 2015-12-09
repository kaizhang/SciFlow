{-# LANGUAGE OverloadedStrings #-}
module Scientific.Workflow.DB
    ( openDB
    , readData
    , saveData
    , isFinished
    , getKeys
    ) where

import Scientific.Workflow.Types
import           Shelly              (fromText, lsT, shelly, test_f, mkdir_p)
import qualified Data.ByteString     as B
import qualified Data.Text           as T

openDB :: FilePath -> IO WorkflowDB
openDB dir = do
    shelly $ mkdir_p $ fromText $ T.pack dir
    return $ WorkflowDB dir
{-# INLINE openDB #-}

readData :: Serializable r => PID -> WorkflowDB -> IO r
readData p (WorkflowDB dir) = deserialize <$> B.readFile fl
  where fl = dir ++ "/" ++ T.unpack p
{-# INLINE readData #-}

saveData :: Serializable r => PID -> r -> WorkflowDB -> IO ()
saveData p result (WorkflowDB dir) = B.writeFile fl $ serialize result
  where fl = dir ++ "/" ++ T.unpack p
{-# INLINE saveData #-}

isFinished :: PID -> WorkflowDB -> IO Bool
isFinished p (WorkflowDB dir) = shelly $ test_f $ fromText $ T.pack fl
  where fl = dir ++ "/" ++ T.unpack p
{-# INLINE isFinished #-}

getKeys :: WorkflowDB -> IO [PID]
getKeys (WorkflowDB dir) = f <$> shelly (lsT $ fromText $ T.pack dir)
  where
    f = map (snd . T.breakOnEnd "/")
{-# INLINE getKeys #-}
