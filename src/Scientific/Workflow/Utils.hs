module Scientific.Workflow.Utils where

import Control.Arrow
import qualified Data.Text as T
import           Shelly    (fromText, shelly, test_f)

fileExist :: FilePath -> IO Bool
fileExist x = shelly $ test_f $ fromText $ T.pack x
{-# INLINE fileExist #-}

mapA :: Monad m => Kleisli m a b -> Kleisli m [a] [b]
mapA (Kleisli f) = Kleisli $ mapM f
{-# INLINE mapA #-}
