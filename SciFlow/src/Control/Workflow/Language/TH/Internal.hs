{-# LANGUAGE TemplateHaskell #-}

module Control.Workflow.Language.TH.Internal
  ( selectInput
  , replicateOutput
  , linkFunctions
  ) where

import Control.Arrow
import           Language.Haskell.TH

-- selectInput :: [Int] -> a (b,c) d -> a (...,b,...,c...) (d,...)
selectInput :: Int     -- ^ Number of input variables
            -> [Int]   -- ^ Input positions
            -> ExpQ    -- ^ Arrow function
            -> ExpQ
selectInput 1 [0] f = f 
selectInput n idx f = [| arr $orderInput >>> first $f >>> arr $orderOutput |]
  where
    orderInput = return $ LamE [TupP $ map VarP vars] $
        TupE [ tupE' $ map (Just . VarE) output1
             , Just $ TupE $ map (Just . VarE) output2 ]
    output1 = map (vars!!) idx
    output2 = map snd $ filter (not . (`elem` idx) . fst) $ zip [0..] vars
    vars = map (\i -> mkName $ "x" ++ show i) [1..n]
    orderOutput = return $ LamE [TupP [VarP o, TupP $ map VarP output2]] outExp
      where
        outExp | null output2 = VarE o
               | otherwise = TupE $ Just (VarE o) : map (Just . VarE) output2
        o = mkName "o"
    tupE' [x] = x
    tupE' xs = Just $ TupE xs
{-# INLINE selectInput #-}

-- outputN n = arr (\(x,y,...) -> (x,...,x,y,...))
replicateOutput :: Int   -- ^ Number of input variables
                -> Int   -- ^ replication times
                -> ExpQ
replicateOutput _ 1 = [| arr id |]
replicateOutput n m = [| arr $f |]
  where
    f = return $ LamE [tupP' vars] $ TupE $ map (Just . VarE) $
        replicate m (head vars) ++ tail vars
    vars = map (\i -> mkName $ "x" ++ show i) [1..n]
    tupP' [x] = VarP x
    tupP' xs = TupP $ map VarP xs
{-# INLINE replicateOutput #-}

linkFunctions :: [ExpQ] -> ExpQ
linkFunctions [] = error "Input is empty"
linkFunctions [x] = x
linkFunctions (x:xs) = [| $x >>> $(linkFunctions xs) |]
{-# INLINE linkFunctions #-}