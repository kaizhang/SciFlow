{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Functions
    ( input
    , plus1
    , combine
    , builder
    ) where

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
    node "id000" [| return <$> input |] def
    node "id001" [| return <$> plus1 |] def
    node "id002" [| return <$> (*2) |] def
    node "id003" [| return <$> combine |] def
    node "id004" [| return <$> mul2 |] def

    ["id000"] ~> "id001"
    ["id000"] ~> "id002"
    ["id001", "id002"] ~> "id003"
    ["id002"] ~> "id004"
