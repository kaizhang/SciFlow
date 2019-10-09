{-# LANGUAGE RecordWildCards #-}
module Control.Workflow.Main.Command
    ( SubParser(..)
    , mkArgsParser
    , runParser
    , viewParser
    , remoteParser
    , deleteParser
    , showParser
    ) where

import           Options.Applicative

import Control.Workflow.Main.Types
import Control.Workflow.Main.Command.Run
import Control.Workflow.Main.Command.View
import Control.Workflow.Main.Command.Remote
import Control.Workflow.Main.Command.Delete
import Control.Workflow.Main.Command.Show
import Data.Proxy (Proxy(..))

import Control.Workflow.Coordinator

data SubParser a = SubParser
    { _subparser_name :: String   -- ^ Name of the command.
    , _subparser_desc :: String   -- ^ Description of the command.
    , _subparser      :: Parser a -- ^ Arguments parser.
    }

runParser :: Coordinator coord
          => (String -> Int -> FilePath -> IO (Config coord))  -- ^ Config reader
          -> SubParser Command
runParser f = SubParser
    { _subparser_name = "run"
    , _subparser_desc = "Run workflow"
    , _subparser      = run f }

viewParser :: SubParser Command
viewParser = SubParser
    { _subparser_name = "view"
    , _subparser_desc = "Produce HTML visualization of the workflow"
    , _subparser      = view }

remoteParser :: Coordinator coord => Proxy coord -> SubParser Command
remoteParser proxy = SubParser
    { _subparser_name = "remote"
    , _subparser_desc = "Run workflow in the worker mode"
    , _subparser      = remote proxy }

deleteParser :: SubParser Command
deleteParser = SubParser
    { _subparser_name = "delete"
    , _subparser_desc = "Delete node cache"
    , _subparser      = delete }

showParser :: SubParser Command
showParser = SubParser
    { _subparser_name = "show"
    , _subparser_desc = "Show node cache"
    , _subparser      = show' }

mkArgsParser :: String   -- ^ Header of the Program helper.
             -> String   -- ^ Description
             -> [SubParser a] -> ParserInfo a
mkArgsParser h descr cmd = info (helper <*> parser) $ fullDesc <> header h <> progDesc descr
  where
    parser = subparser $ mconcat $ map mkSubParser cmd

mkSubParser :: SubParser a -> Mod CommandFields a
mkSubParser SubParser{..} = command _subparser_name $
    info (helper <*> _subparser) $ fullDesc <> progDesc _subparser_desc
{-# INLINE mkSubParser #-}
 