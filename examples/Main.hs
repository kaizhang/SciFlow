{-# LANGUAGE TemplateHaskell #-}
import Scientific.Workflow
import qualified Functions as F
import Data.Default

-- assemble workflow using template haskell
$(mkWorkflow "myWorkflow" def{_overwrite=False} F.builder)

main = runWorkflow myWorkflow
