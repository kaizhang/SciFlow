{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Functions
    ( input
    , plus1
    , combine
    , builder
    ) where

import Control.Lens
import Scientific.Workflow

input :: () -> Int
input = const 10

plus1 :: Int -> Int
plus1 = (+1)

mul2 :: Int -> Int
mul2 = (*2)

combine :: (Int, Int) -> Int
combine = \(a,b) -> 2 * (a + b)

-- builder monad
builder :: Builder ()
builder = do
    node "id000" [| return <$> input |] $ note .= "input"
    node "id001" [| return <$> plus1 |] $ note .= "+ 1"
    node "id002" [| return <$> (*2) |] $ note .= "* 2"
    node "id003" [| return <$> combine |] $ note .= "+"
    node "id004" [| return <$> mul2 |] $ note .= "* 2"

    ["id000"] ~> "id001"
    ["id000"] ~> "id002"
    ["id001", "id002"] ~> "id003"
    ["id002"] ~> "id004"
