module Control.Workflow.Main.Command
    ( mkArgsParser
    , run
    , view
    , remote
    ) where

import           Options.Applicative

import Control.Workflow.Main.Types
import Control.Workflow.Main.Command.Run
import Control.Workflow.Main.Command.View
import Control.Workflow.Main.Command.Remote

mkArgsParser :: String   -- ^ Header
             -> [(String, Parser a)] -> ParserInfo a
mkArgsParser h cmd = info (helper <*> parser) $ fullDesc <> header h
  where
    parser = subparser $ mconcat $ map (uncurry mkSubParser) cmd

mkSubParser :: String -> Parser a -> Mod CommandFields a
mkSubParser name parser = command name $ info (helper <*> parser) $
    fullDesc <> progDesc "run workflow"
 