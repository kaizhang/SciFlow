Scientific workflow management system
=====================================

A scientific workflow is a series of computational steps which usually can be presented as a Directed Acyclic Graph (DAG).

SciFlow is to help programmers design complex workflows with ease. Here is a trivial example. Since we use template haskell, we need to divide this small program into two files.

```haskell
-- File 1: MyModule.hs
---------------------------------------------------

{-# LANGUAGE OverloadedStrings #-}
module MyModule where

import Control.Arrow
import Scientific.Workflow

input :: Actor () Int
input = arr $ const 10

plus1 :: Actor Int Int
plus1 = arr (+1)

mul2 :: Actor Int Int
mul2 = arr (*2)

combine :: Actor (Int, Int) Int
combine = arr $ \(a,b) -> a + b

-- builder monad
builder :: Builder ()
builder = do
    node "id000" "input" "this is input"
    node "id001" "plus1" "add 1 to the input"
    node "id002" "mul2" "doulbe the input"
    node "id003" "combine" "combine two input"
    
    "id000" ~> "id001"
    "id000" ~> "id002"
    link2 ("id001", "id002") "id003"


-- File 2: main.hs

{-# LANGUAGE TemplateHaskell #-}
import Scientific.Workflow
import MyModule
import Data.Default

-- assemble workflow using template haskell
$(mkWorkflow "myWorkflow" builder)

main = do result <- runWorkflow myWorkflow def
          print result
```
