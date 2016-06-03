{-# LANGUAGE TemplateHaskell #-}

import System.Environment

import qualified Functions as F
import qualified Data.Text.Lazy.IO as T

import Scientific.Workflow.Main

defaultMain F.builder
