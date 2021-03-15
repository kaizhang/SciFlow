{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
module Control.Workflow.DataStore
    ( DataStore(..)
    , Key(..)
    , showKey
    , mkKey
    , JobStatus(..)
    , openStore
    , closeStore
    , withStore
    , saveEnv
    , readEnv
    , queryStatus
    , saveItem
    , fetchItem
    , fetchItems
    , delItemByID
    , delItems
    ) where

import Control.Monad (unless)
import Control.Concurrent.MVar (withMVar, newMVar, MVar)
import Control.Monad.Catch (MonadMask, bracket)
import Data.Binary (Binary, encode, decode)
import Data.Hashable (Hashable(..))
import Data.Typeable (Typeable, typeOf)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Database.SQLite.Simple
import GHC.Generics (Generic)
import qualified Data.Text as T
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import qualified Crypto.Hash.SHA256 as C
import Data.ByteArray.Encoding (convertToBase, Base(..))

-- | A data store consists of an in-memory job status table and a on-disk 
-- cache.
newtype DataStore = DataStore (MVar Connection)

-- | A key is uniquely determined by the input hash and the name of the job.
data Key = Key { _hash :: B.ByteString, _name :: T.Text }

showKey :: Key -> String
showKey (Key h jn) = T.unpack jn <> "(" <> B.unpack h <> ")"

instance Eq Key where
    a == b = _hash a == _hash b

instance Hashable Key where
    hashWithSalt s = hashWithSalt s . _hash

instance Show Key where
    show (Key h jn) = T.unpack jn <> "(" <> h' <> ")"
      where
        h' = B.unpack (B.take 4 h) <> ".."

-- | Create the key from input and job name.
mkKey :: (Typeable i, Binary i) => i -> T.Text -> Key
mkKey input nm = Key h nm
  where
    h = convertToBase Base16 $ C.hashlazy $ encode (nm, show $ typeOf input, input)
{-# INLINE mkKey #-}

-- | The status of jobs.
data JobStatus = Complete BL.ByteString
               | Failed String
               | Missing
               deriving (Eq, Generic, Show)

instance Binary JobStatus

-- | Open the store.
openStore :: MonadIO m => FilePath -> m DataStore
openStore root = liftIO $ do
    db <- open root
    itemExist <- hasTable db "item_db"
    unless itemExist $ do
        execute_ db "CREATE TABLE item_db(hash TEXT PRIMARY KEY, jobname TEXT, data BLOB)"
        execute_ db "CREATE INDEX jobname_index ON item_db(jobname)"
    DataStore <$> newMVar db
{-# INLINE openStore #-}

-- | Close the store and release resource.
closeStore :: MonadIO m => DataStore -> m ()
closeStore (DataStore s) = liftIO $ withMVar s close
{-# INLINE closeStore #-}

withStore :: (MonadIO m, MonadMask m)
          => FilePath -> (DataStore -> m a) -> m a
withStore root = bracket (openStore root) closeStore
{-# INLINE withStore #-}

envKey :: Key
envKey = Key "SciFlow.configuration" "SciFlow.configuration"
{-# INLINE envKey #-}

saveEnv :: (Binary env, MonadIO m) => DataStore -> env -> m ()
saveEnv store = saveItem store envKey
{-# INLINE saveEnv #-}

readEnv :: (Binary env, MonadIO m) => DataStore -> m (Maybe env)
readEnv store = fetchItem store envKey
{-# INLINE readEnv #-}

-- | Get the status of a given job
queryStatus :: MonadIO m => DataStore -> Key -> m JobStatus
queryStatus (DataStore store) k = liftIO $ withMVar store $ \db ->
    fetchItemBS db k >>= \case
        Nothing -> return Missing
        Just dat -> return $ Complete dat
{-# INLINE queryStatus #-}

-- | Save the data of a given job.
saveItem :: (MonadIO m, Binary a) => DataStore -> Key -> a -> m ()
saveItem (DataStore store) (Key k n) res = liftIO $ withMVar store $ \db ->
    execute db "REPLACE INTO item_db VALUES (?, ?, ?)" (k, n, encode res)
{-# INLINE saveItem #-}

-- | Get the data of a given job.
fetchItem :: (MonadIO m, Binary a) => DataStore -> Key -> m (Maybe a)
fetchItem (DataStore store) key = liftIO $ withMVar store $ \db ->
    (fmap . fmap) decode $ fetchItemBS db key
{-# INLINE fetchItem #-}

-- | Get the data of a given job.
fetchItemBS :: MonadIO m => Connection -> Key -> m (Maybe BL.ByteString)
fetchItemBS db (Key k _) = liftIO $
    query db "SELECT data FROM item_db WHERE hash=?" [k] >>= \case
        [Only result] -> return $ Just result
        _ -> return Nothing
{-# INLINE fetchItemBS #-}

-- | Given a job name, return a list of items associated with the job.
fetchItems :: (MonadIO m, Binary a) => DataStore -> T.Text -> m [a]
fetchItems (DataStore store) jn = liftIO $ withMVar store $ \db ->
    map (\(Only res) -> decode res) <$>
        query db "SELECT data FROM item_db WHERE jobname=?" [jn] 
{-# INLINE fetchItems #-}

-- | Delete a record based on the fingerprint.
delItemByID :: MonadIO m
            => DataStore
            -> B.ByteString
            -> m ()
delItemByID (DataStore store) k = liftIO $ withMVar store $ \db ->
    execute db "DELETE FROM item_db WHERE hash= ?" [k]
{-# INLINE delItemByID #-}

-- | Delete all records with the given job name.
delItems :: MonadIO m
         => DataStore
         -> T.Text     -- ^ job name
         -> m ()
delItems (DataStore store) jn = liftIO $ withMVar store $ \db ->
    execute db "DELETE FROM item_db WHERE jobname= ?" [jn]
{-# INLINE delItems #-}

-------------------------------------------------------------------------------
-- Low level functions
-------------------------------------------------------------------------------

{-
-- | Get all completed jobs.
completedJobs :: MonadIO m => Connection -> m [(Key, BL.ByteString)]
completedJobs db = liftIO $ do
    r <- query_ db "SELECT hash,jobname,data FROM item_db"
    return $ flip map r $ \(h,n,dat) -> (Key h n, dat)
{-# INLINE completedJobs #-}
-}

hasTable :: Connection -> String -> IO Bool
hasTable db tablename = do
    r <- query db
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?" [tablename]
    return $ not $ null (r :: [Only T.Text])
{-# INLINE hasTable #-}

{-
getKeys :: Connection -> IO [Key]
getKeys db = concat <$> query_ db
    (Query $ T.pack $ printf "SELECT pid FROM %s;" dbTableName)
-}