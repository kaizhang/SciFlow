{-# LANGUAGE TemplateHaskell #-}
module Scientific.Workflow.Main where

import Options.Applicative
import Options.Applicative.Types
import Data.List.Split (splitOn)
import qualified Data.HashMap.Strict as M
import Language.Haskell.TH

import Scientific.Workflow

workflowOptions :: Parser RunOpt
workflowOptions = subparser $
    command "run" ( info (helper <*> ( RunOpt
                                   <$> strOption
                                           ( long "dir"
                                          <> value "./"
                                          <> short 'd' )
                                   <*> strOption
                                           ( long "log"
                                          <> value "wfCache"
                                          <> short 'l' )
                                      {-
                                   <*> switch
                                           (long "overwrite")
                                   <*> option readerMode
                                           ( long "nodes"
                                          <> value All )
                                      -}
                                     ) ) $
                         fullDesc
                      <> progDesc "run"
                      )

{-
readerMode :: ReadM Mode
readerMode = Select . splitOn "," <$> readerAsk
{-# INLINE readerMode #-}
-}

defaultMain :: Builder () -> Q [Dec]
defaultMain builder = do
    workflowDec <- mkWorkflow "workflow_main" builder
    mainDec <- [d| $(varP $ mkName "main") = execParser
                       (info (helper <*> workflowOptions) fullDesc) >>=
                       runWorkflow $(varE $ mkName "workflow_main")
               |]
    return $ workflowDec ++ mainDec
