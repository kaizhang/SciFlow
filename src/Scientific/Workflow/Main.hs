{-# LANGUAGE TemplateHaskell #-}
module Scientific.Workflow.Main where

import Options.Applicative
import Options.Applicative.Types
import Data.List.Split (splitOn)
import Language.Haskell.TH

import Scientific.Workflow

workflowOptions :: Parser RunOpt
workflowOptions = subparser $
    command "run" ( info (helper <*> runOptParser) $ fullDesc <> progDesc "run" )
  where
    runOptParser = RunOpt
        <$> strOption
            ( long "dir"
           <> value "./"
           <> short 'd' )
        <*> strOption
            ( long "log"
           <> value "wfCache"
           <> short 'l' )
        <*> option (Select . splitOn "," <$> readerAsk)
            ( long "nodes"
           <> value All )
        <*> switch
            ( long "force"
           <> short 'f' )

defaultMain :: Builder () -> Q [Dec]
defaultMain builder = do
    workflowDec <- mkWorkflow "workflow_main" builder
    mainDec <- [d| $(varP $ mkName "main") = execParser
                       (info (helper <*> workflowOptions) fullDesc) >>=
                       runWorkflow $(varE $ mkName "workflow_main")
               |]
    return $ workflowDec ++ mainDec
