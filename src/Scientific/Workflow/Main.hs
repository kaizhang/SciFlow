{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveLift #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleInstances #-}

module Scientific.Workflow.Main where

import Scientific.Workflow
import Scientific.Workflow.Visualize
import qualified Language.Haskell.TH.Lift as T
import           Language.Haskell.TH
import System.Environment
import qualified Data.Text.Lazy.IO as T
import Data.Graph.Inductive.Graph (nmap, mkGraph, labNodes, labEdges)
import Data.Graph.Inductive.PatriciaTree (Gr(..))

deriving instance T.Lift Attribute

instance T.Lift (Gr (PID, Attribute) Int) where
  lift gr = [| uncurry mkGraph $(T.lift (labNodes gr, labEdges gr)) |]

defaultMain :: Builder () -> Q [Dec]
defaultMain builder = do
    wf_q <- buildWorkflow wfName builder
    main_q <- [d| main = mainFunc dag $(varE $ mkName wfName) |]
    return $ wf_q ++ main_q
  where
    wfName = "sciFlowDefaultMain"
    dag = nmap (\(a,(_,b)) -> (a,b)) $ mkDAG builder

mainFunc :: Gr (PID, Attribute) Int -> [Workflow] -> IO ()
mainFunc dag wf = do
    (cmd:args) <- getArgs
    case cmd of
        "run" -> runWorkflow wf def
        "view" -> T.putStrLn $ renderBuilder dag
