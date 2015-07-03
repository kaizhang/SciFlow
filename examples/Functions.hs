{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Functions
    ( input
    , plus1
    , combine
    , builder
    ) where

import Control.Arrow
import Scientific.Workflow

input :: () -> Int
input = const 10

plus1 :: Int -> Int
plus1 = (+1)

--mul2 :: Int -> Int
--mul2 = (*2)

combine :: (Int, Int) -> Int
combine = \(a,b) -> 2 * (a + b)

-- builder monad
builder :: Builder ()
builder = do
    node "id000" 'input "this is input"
    node "id001" 'plus1 "add 1 to the input"
--    node "id002" 'mul2 "double the input"
    node "id002" [| (*2) |] ""
    node "id003" 'combine "combine two input"

    ["id000"] ~> "id001"
    ["id000"] ~> "id002"
    ["id001", "id002"] ~> "id003"
