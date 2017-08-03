{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}

import System.Environment

import Functions (builder)
import qualified Data.Text.Lazy.IO as T

import Scientific.Workflow.Main
import Scientific.Workflow.Internal.Builder

--defaultMain builder
defaultMain $ namespace "prefix" builder
