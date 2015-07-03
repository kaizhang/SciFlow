module Scientific.Workflow.Utils where

import qualified Data.Text as T
import           Shelly    (fromText, shelly, test_f)

fileExist :: FilePath -> IO Bool
fileExist x = shelly $ test_f $ fromText $ T.pack x
{-# INLINE fileExist #-}
