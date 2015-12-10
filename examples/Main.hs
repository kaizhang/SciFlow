{-# LANGUAGE TemplateHaskell #-}
import Scientific.Workflow
import qualified Functions as F

buildWorkflow "wf" F.builder

main :: IO ()
main = runWorkflow wf def
