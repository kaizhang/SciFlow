{-# LANGUAGE TemplateHaskell #-}

module Control.Workflow.Language.TH.Internal
  ( link
  , selectInput
  , replicateOutput
  , linkFunctions
  ) where

import Control.Arrow
import           Language.Haskell.TH

link :: [String]  -- a list of parents
     -> ExpQ      -- child
     -> ExpQ
link [] x = x
link [s] x = [| $(varE $ mkName s) >>> $x |]
link [s1,s2] x = [| linkA2 $(varE $ mkName s1) $(varE $ mkName s2) $x |]
link [s1,s2,s3] x = [| linkA3 $(varE $ mkName s1) $(varE $ mkName s2) 
    $(varE $ mkName s3) $x |]
link [s1,s2,s3,s4] x = [| linkA4 $(varE $ mkName s1) $(varE $ mkName s2) 
    $(varE $ mkName s3) $(varE $ mkName s4) $x |]
link [s1,s2,s3,s4,s5] x = [| linkA5 $(varE $ mkName s1) $(varE $ mkName s2) 
    $(varE $ mkName s3) $(varE $ mkName s4) $(varE $ mkName s5) $x |]
link xs x = linkAN (map mkName xs) x
{-# INLINE link #-}

linkA2 :: Arrow arr => arr a b1 -> arr a b2 -> arr (b1, b2) c -> arr a c
linkA2 a1 a2 f = (a1 &&& a2) >>> f
{-# INLINE linkA2 #-}

linkA3 :: Arrow arr => arr a b1 -> arr a b2 -> arr a b3
       -> arr (b1, b2, b3) c
       -> arr a c
linkA3 a1 a2 a3 f = (a1 &&& a2 &&& a3) >>>
    arr (\(b1,(b2,b3)) -> (b1,b2,b3)) >>> f
{-# INLINE linkA3 #-}

linkA4 :: Arrow arr => arr a b1 -> arr a b2 -> arr a b3 -> arr a b4
       -> arr (b1, b2, b3, b4) c
       -> arr a c
linkA4 a1 a2 a3 a4 f = (a1 &&& a2 &&& a3 &&& a4) >>>
    arr (\(b1,(b2,(b3,b4))) -> (b1,b2,b3,b4)) >>> f
{-# INLINE linkA4 #-}

linkA5 :: Arrow arr => arr a b1 -> arr a b2 -> arr a b3 -> arr a b4 -> arr a b5
       -> arr (b1, b2, b3, b4, b5) c
       -> arr a c
linkA5 a1 a2 a3 a4 a5 f = (a1 &&& a2 &&& a3 &&& a4 &&& a5) >>>
    arr (\(b1,(b2,(b3,(b4,b5)))) -> (b1,b2,b3,b4,b5)) >>> f
{-# INLINE linkA5 #-}

linkAN :: [Name]  -- ^ a list of Arrows
       -> ExpQ
       -> ExpQ
linkAN as f = [| $arr1 >>> arr $arr2 >>> $f |]
  where
    arr1 = return $ foldr1 g $ map VarE as
      where
        g x1 x2 = AppE (AppE (VarE '(&&&)) x1) x2
    arr2 = return $ LamE [tuple1] $ TupE $ map (Just . VarE) vars
      where
        tuple1 = go $ map VarP vars
          where
            go [x] = x
            go (x:xs) = TupP [x, go xs]
            go _ = error "empty list"
        vars = map (\i -> mkName $ "x" ++ show i) ([1..n] :: [Int])
    n = length as
{-# INLINE linkAN #-}


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