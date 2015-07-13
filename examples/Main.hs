{-# LANGUAGE TemplateHaskell #-}
import Scientific.Workflow.Main
import Scientific.Workflow
import qualified Functions as F
import Data.Default

defaultMain F.builder
