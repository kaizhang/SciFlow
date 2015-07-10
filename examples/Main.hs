{-# LANGUAGE TemplateHaskell #-}
import Scientific.Workflow.Main
import qualified Functions as F
import Data.Default

{-
-- assemble workflow using template haskell
$(mkWorkflow "myWorkflow" def{_overwrite=False} F.builder)
-}

defaultMain F.builder
--main = runWorkflow myWorkflow
