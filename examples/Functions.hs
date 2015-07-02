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

input :: Actor () Int
input = arr $ const 10

plus1 :: Actor Int Int
plus1 = arr (+1)

--mul2 :: Actor Int Int
--mul2 = arr (*2)

combine :: Actor (Int, Int) Int
combine = arr $ \(a,b) -> a + b

-- builder monad
builder :: Builder ()
builder = do
    node "id000" 'input "this is input"
    node "id001" 'plus1 "add 1 to the input"
--    node "id002" 'mul2 "double the input"
    node "id002" [| arr (*2) |] ""
    node "id003" 'combine "combine two input"

    ["id000"] ~> "id001"
    ["id000"] ~> "id002"
    ["id001", "id002"] ~> "id003"
