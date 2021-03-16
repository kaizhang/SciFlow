{-# LANGUAGE TemplateHaskell #-}

module Control.Workflow.Language.TH.Internal
  ( selectInput
  , replicateOutput
  , linkFunctions
  , combineArrows
  ) where

import Control.Arrow
import           Language.Haskell.TH

combineArrows :: [(Int, ExpQ, Int)]   -- ^ Arrows each with type: a (b,c,...) (d,e,..)
              -> ExpQ     -- ^ Final arrow with type a (b,c,...,b',c',...) (d,d',...)
combineArrows arrows = let (_,e,_) = foldl1 merge arrows in e
  where
    merge (i1, a1, o1) (i2, a2, o2) =
        (i1 + i2, [| arr $(unflatten i1 i2) >>> ($a1 *** $a2) >>> arr $(flatten o1 o2) |], o1 + o2)
    unflatten n m = return $ LamE [tupP' $ vxs ++ vys] $ 
        TupE [Just $ tupE' vxs, Just $ tupE' vys]
      where
        vxs = map (\i -> mkName $ "x" ++ show i) [1..n]
        vys = map (\i -> mkName $ "y" ++ show i) [1..m]
    flatten n m = return $ LamE [TupP [tupP' vxs, tupP' vys]] $
        TupE $ map (Just . VarE) $ vxs ++ vys
      where
        vxs = map (\i -> mkName $ "x" ++ show i) [1..n]
        vys = map (\i -> mkName $ "y" ++ show i) [1..m]
{-# INLINE combineArrows #-}
  

-- selectInput :: [Int] -> a (b,c) (d,e) -> a (...,b,...,c...) (d,e,...)
selectInput :: Int     -- ^ Number of input variables
            -> Int     -- ^ Number of output variables
            -> [Int]   -- ^ Input positions
            -> ExpQ    -- ^ Arrow function
            -> ExpQ
selectInput 1 _ [0] f = f 
selectInput nIn nOut idx f = [| arr $orderInput >>> first $f >>> arr $orderOutput |]
  where
    orderInput = return $ LamE [tupP' vars] $ TupE $
        map (Just . tupE') [output1, output2]
    output1 = map (vars!!) idx
    output2 = map snd $ filter (not . (`elem` idx) . fst) $ zip [0..] vars
    vars = map (\i -> mkName $ "x" ++ show i) [1..nIn]
    orderOutput = return $ LamE [TupP [tupP' outVars, tupP' output2]] $
        tupE' $ outVars <> output2
      where
        outVars = map (\i -> mkName $ "o" ++ show i) [1..nOut]
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
{-# INLINE replicateOutput #-}

linkFunctions :: [ExpQ] -> ExpQ
linkFunctions [] = error "Input is empty"
linkFunctions [x] = x
linkFunctions (x:xs) = [| $x >>> $(linkFunctions xs) |]
{-# INLINE linkFunctions #-}

tupP' :: [Name] -> Pat
tupP' [x] = VarP x
tupP' xs = TupP $ map VarP xs

tupE' :: [Name] -> Exp
tupE' [x] = VarE x
tupE' xs = TupE $ map (Just . VarE) xs

-- | Turn function: (a,b) -> c into (Maybe a, Maybe b) -> Maybe c
wrapMaybe :: Int -> ExpQ -> ExpQ
wrapMaybe n f = return $ LamE [tupP' vars] [| |]
  where
    vars = map (\i -> mkName $ "x" ++ show i) [1..n]