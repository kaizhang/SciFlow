module Control.Workflow
    ( SciFlowOpts(..)
    , defaultOpts
    , module Control.Workflow.Types
    , module Control.Workflow.Language
    , module Control.Workflow.Language.TH
    ) where

import qualified Data.HashMap.Strict as M

import Control.Workflow.Types
import Control.Workflow.Language
import Control.Workflow.Language.TH

data SciFlowOpts = SciFlowOpts
    { _store_path  :: FilePath
    , _master_addr :: String
    , _master_port :: Int
    , _n_workers   :: Int
    , _resources   :: ResourceConfig }

defaultOpts :: SciFlowOpts
defaultOpts = SciFlowOpts
    { _store_path = "sciflow.db"
    , _master_addr = "192.168.0.1"
    , _master_port = 8888
    , _n_workers = 5
    , _resources = ResourceConfig M.empty }