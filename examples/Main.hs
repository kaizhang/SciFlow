{-# LANGUAGE TemplateHaskell #-}
import Scientific.Workflow.Main
import Scientific.Workflow
import qualified Functions as F
import Data.Default

{-
mkWorkflow "my" F.builder

main = defaultMain my
-}

defaultMain F.builder
