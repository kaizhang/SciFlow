{-# LANGUAGE TemplateHaskell #-}
import Scientific.Workflow
import qualified Functions as F

buildWorkflowPart "wf" F.builder def

main :: IO ()
main = runWorkflow wf def
