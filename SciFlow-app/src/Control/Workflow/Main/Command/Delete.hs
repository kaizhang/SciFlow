{-# LANGUAGE RecordWildCards #-}

module Control.Workflow.Main.Command.Delete (delete) where

import qualified Data.Text as T
import qualified Data.ByteString.Char8 as B
import           Options.Applicative
import Control.Workflow.DataStore
import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S
import qualified Data.Graph.Inductive as G
import Data.List (foldl')
import Data.Maybe
import Data.Hashable (hash)

import Control.Workflow.Main.Types
import Control.Workflow.Types

data Delete = Delete
    { input :: [T.Text]
    , delDepend :: Bool
    , byId :: Bool
    , dbPath :: FilePath }

instance IsCommand Delete where
    runCommand Delete{..} flow = withStore dbPath $ \store -> if byId
        then mapM_ (delItemByID store) $ map (B.pack . T.unpack) input
        else let nodes | delDepend = findChildren input $ _graph flow
                       | otherwise = input
              in mapM_ (delItems store) nodes

findChildren :: [T.Text] -> G.Gr (Maybe NodeLabel) () -> [T.Text]
findChildren ids gr = mapMaybe f $ S.toList $ go S.empty $ map hash ids
  where
    f i = fmap _label $ fromJust $ G.lab gr i
    go acc [] = acc 
    go acc xs = go (foldl' (flip S.insert) acc xs) parents
      where
        parents = concatMap (G.suc gr) xs
{-# INLINE findChildren #-}

delete :: Parser Command
delete = fmap Command $ Delete
    <$> (some . strArgument)
        ( metavar "STEP1 [STEP2] [STEP3]")
    <*> switch
        ( long "delete-all"
       <> help "Also delete nodes whose results depend on the input node. (default: False)" )
    <*> switch
        ( long "by-id"
       <> help "The input is node id (default: False)" )
    <*> strOption
        ( long "db-path"
       <> value "sciflow.db"
       <> help "Path to the workflow cache. (default: sciflow.db)"
       <> metavar "DB_PATH" )