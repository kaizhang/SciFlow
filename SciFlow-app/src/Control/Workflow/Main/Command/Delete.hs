{-# LANGUAGE RecordWildCards #-}

module Control.Workflow.Main.Command.Delete (delete) where

import qualified Data.Text as T
import qualified Data.ByteString.Char8 as B
import           Options.Applicative
import Control.Workflow.DataStore
import Control.Workflow.Interpreter.Graph
import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S
import Data.List (foldl')

import Control.Workflow.Main.Types

data Delete = Delete
    { input :: [T.Text]
    , delDepend :: Bool
    , byId :: Bool
    , dbPath :: FilePath }

instance IsCommand Delete where
    runCommand Delete{..} flow = withStore dbPath $ \store -> if byId
        then mapM_ (delItemByID store) $ map (B.pack . T.unpack) input
        else let nodes | delDepend = findChildren input $ mkGraph flow
                       | otherwise = input
              in mapM_ (delItems store) nodes

findChildren :: [T.Text] -> Graph -> [T.Text]
findChildren ids gr = S.toList $ go S.empty ids
  where
    go acc [] = acc 
    go acc xs = go (foldl' (flip S.insert) acc xs) children
      where
        children = flip concatMap xs $ \i -> M.lookupDefault [] i gr'
    gr' = M.fromListWith (++) $ map (\e -> (_from e, [_to e])) $ _edges gr

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