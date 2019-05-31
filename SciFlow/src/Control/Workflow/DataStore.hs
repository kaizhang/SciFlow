{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
module Control.Workflow.DataStore
    ( DataStore(..)
    , Key
    , mkKey
    , JobStatus(..)
    , openStore
    , closeStore
    , withStore
    , setStatus
    , queryStatusPending
    , saveItem
    , fetchItem
    , fetchItems
    , delItem
    , delItems
    ) where

import Control.Monad (unless)
import Control.Concurrent.MVar (withMVar, newMVar, MVar, modifyMVar, modifyMVar_)
import qualified Data.HashMap.Strict as M
import Control.Monad.Catch (MonadMask, bracket)
import Data.Binary (Binary, encode, decode)
import Data.Hashable (Hashable(..))
import Data.Typeable (Typeable, typeOf)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Database.SQLite.Simple
import GHC.Generics (Generic)
import qualified Data.Text as T
import qualified Data.ByteString.Char8 as B
import qualified Crypto.Hash.SHA256 as C
import Data.ByteArray.Encoding (convertToBase, Base(..))

-- | A data store consists of an in-memory job status table and a on-disk 
-- cache.
newtype DataStore = DataStore (MVar InternalStore)

data InternalStore = InternalStore
    { _db_conn :: Connection
    , _db_status :: M.HashMap Key JobStatus }

-- | A key is uniquely determined by the input hash and the name of the job.
data Key = Key { _hash :: B.ByteString, _name :: T.Text }

instance Eq Key where
    a == b = _hash a == _hash b

instance Hashable Key where
    hashWithSalt s = hashWithSalt s . _hash

instance Show Key where
    show (Key hash jn) = T.unpack jn <> "(" <> h <> ")"
      where
        h = B.unpack (B.take 4 hash) <> ".."

-- | Create the key from input and job name.
mkKey :: (Typeable i, Binary i) => i -> T.Text -> Key
mkKey input nm = Key hash nm
  where
    hash = convertToBase Base16 $ C.hashlazy $ encode (nm, show $ typeOf input, input)
{-# INLINE mkKey #-}

-- | The status of jobs.
data JobStatus = Complete
               | Failed String
               | Pending
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
    jobs <- completedJobs db
    fmap DataStore $ newMVar $ InternalStore db $
        M.fromList $ zip jobs $ repeat Complete
{-# INLINE openStore #-}

-- | Close the store and release resource.
closeStore :: MonadIO m => DataStore -> m ()
closeStore (DataStore s) = liftIO $ withMVar s $ \(InternalStore db _) -> close db
{-# INLINE closeStore #-}

withStore :: (MonadIO m, MonadMask m)
          => FilePath -> (DataStore -> m a) -> m a
withStore root = bracket (openStore root) closeStore
{-# INLINE withStore #-}

-- | Mark a given job as pending.
setStatus :: MonadIO m => DataStore -> Key -> JobStatus -> m ()
setStatus (DataStore store) k st = liftIO $ modifyMVar_ store $ \db -> 
    let dict = M.insert k st $ _db_status db
    in return $ db{_db_status = dict}
{-# INLINE setStatus #-}

-- | Get the status of a given job and mark the job as pending if missing.
queryStatusPending :: MonadIO m => DataStore -> Key -> m (Maybe JobStatus)
queryStatusPending (DataStore store) k = liftIO $ modifyMVar store $ \db ->
    case M.lookup k (_db_status db) of
        Nothing ->
            let dict = M.insert k Pending $ _db_status db
            in return (db{_db_status = dict}, Nothing)
        Just st -> return (db, Just st)
{-# INLINE queryStatusPending #-}

-- | Save the data of a given job.
saveItem :: (MonadIO m, Binary a) => DataStore -> Key -> a -> m ()
saveItem (DataStore store) (Key k n) res = liftIO $ withMVar store $
    \(InternalStore db _) ->
        execute db "REPLACE INTO item_db VALUES (?, ?, ?)" (k, n, encode res)
{-# INLINE saveItem #-}

-- | Get the data of a given job.
fetchItem :: (MonadIO m, Binary a) => DataStore -> Key -> m a
fetchItem (DataStore store) (Key k _) = liftIO $ withMVar store $
    \(InternalStore db _) ->
        query db "SELECT data FROM item_db WHERE hash=?" [k] >>= \case
            [Only result] -> return $ decode result
            _ -> error "Item not found"
{-# INLINE fetchItem #-}

-- | Given a job name, return a list of items associated with the job.
fetchItems :: (MonadIO m, Binary a) => DataStore -> T.Text -> m [a]
fetchItems (DataStore store) jn = liftIO $ withMVar store $
    \(InternalStore db _) -> map (\(Only res) -> decode res) <$>
        query db "SELECT data FROM item_db WHERE jobname=?" [jn] 
{-# INLINE fetchItems #-}

-- | Delete a record based on the key.
delItem :: MonadIO m => DataStore -> Key -> m ()
delItem (DataStore store) (Key k _) = liftIO $ withMVar store $
    \(InternalStore db _) -> execute db "DELETE FROM item_db WHERE hash= ?" [k]
{-# INLINE delItem #-}

-- | Delete all records with the given job name.
delItems :: MonadIO m
         => DataStore
         -> T.Text     -- ^ job name
         -> m ()
delItems (DataStore store) jn = liftIO $ withMVar store $
    \(InternalStore db _) -> execute db "DELETE FROM item_db WHERE jobname= ?" [jn]
{-# INLINE delItems #-}

-------------------------------------------------------------------------------
-- Low level functions
-------------------------------------------------------------------------------

-- | Get all completed jobs.
completedJobs :: MonadIO m => Connection -> m [Key]
completedJobs db = liftIO $ do
    r <- query_ db "SELECT hash,jobname FROM item_db"
    return $ flip map r $ \(h,n) -> Key h n
{-# INLINE completedJobs #-}

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