{-# LANGUAGE TemplateHaskell #-}
module Scientific.Workflow.Main where

import Options.Applicative
import Options.Applicative.Types
import Data.List.Split (splitOn)
import qualified Data.HashMap.Strict as M
import Language.Haskell.TH

import Scientific.Workflow

workflowOptions :: Parser WorkflowConfig
workflowOptions = subparser $
    command "run" ( info (helper <*> ( WorkflowConfig
                                   <$> strOption
                                           ( long "dir"
                                          <> short 'd' )
                                   <*> strOption
                                           ( long "log"
                                          <> short 'l' )
                                   <*> switch
                                           (long "overwrite")
                                   <*> option readerMode
                                           ( long "nodes"
                                          <> value All )
                                    <*> pure (WorkflowState M.empty)
                                     ) ) $
                         fullDesc
                      <> progDesc "run"
                      )

readerMode :: ReadM Mode
readerMode = Select . splitOn "," <$> readerAsk
{-# INLINE readerMode #-}

defaultMain :: Builder () -> Q [Dec]
defaultMain builder = do
    config <- runIO $ execParser opts
    dec <- mkWorkflow "workflow_main" config builder
    m <- [d| main = runWorkflow $(newName "workflow_main" >>= varE) |]
    return $ dec ++ m
  where
    opts = info (helper <*> workflowOptions) fullDesc
