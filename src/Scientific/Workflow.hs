{-|
Module      : Scientific.Workflow
Description : Building type safe scientific workflows
Copyright   : (c) 2015-2017 Kai Zhang
License     : MIT
Maintainer  : kai@kzhang.org
Stability   : experimental
Portability : POSIX

SciFlow is a DSL for building scientific workflows. Workflows built with SciFlow
can be run either on desktop computers or in grid computing environments that
support DRMAA.

Features:

1. Easy to use and safe: Provide a simple and flexible way to design type safe
computational pipelines in Haskell.

2. Automatic Checkpointing: The states of intermediate steps are automatically
logged, allowing easy restart upon failures.

3. Parallelism and grid computing support.

Example:

> import           Control.Lens             ((.=))
> import           Scientific.Workflow
>
> f :: Int -> Int
> f = (+1)
>
> defaultMain $ do
>     nodeS "step0" [| return . const [1..10] :: () -> WorkflowConfig () [Int] |] $ return ()
>     nodeP' 2 "step1" 'f $ note .= "run in parallel with batch size 2"
>     nodeP' 4 "step2" 'f $ note .= "run in parallel with batch size 4"
>     node' "step3" [| \(x, y) -> x ++ y |] $ return ()
>
>     ["step0"] ~> "step1"
>     ["step0"] ~> "step2"
>     ["step1", "step2"] ~> "step3"

-}
module Scientific.Workflow
    ( defaultMain
    , mainWith
    , defaultMainOpts
    , MainOpts(..)

    , Builder
    , namespace
    , node
    , node'
    , nodeS
    , nodeP
    , nodeP'
    , nodePS
    , nodeSharedP
    , nodeSharedP'
    , nodeSharedPS
    , link
    , (~>)
    , path

    , label
    , note
    , submitToRemote
    , remoteParam

    , ContextData(..)
    , WorkflowConfig
    ) where

import           Scientific.Workflow.Internal.Builder
import           Scientific.Workflow.Internal.Builder.Types
import           Scientific.Workflow.Main
import           Scientific.Workflow.Types
