{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

import Path
import Scientific.Workflow
import Scientific.Workflow.Types
import Scientific.Workflow.Exec
import Scientific.Workflow.Coordinator.Drmaa
import qualified Control.Funflow.ContentStore                as CS
import Data.Default
import           Language.Haskell.TH
import Control.Arrow
import Control.Funflow

compile "wf" [t| SciFlow IO Int () |] $
    node "S1" [| (*2) |] $
    node "S2" [| (+2) |] $
    ["S1", "S2"] ~> "S3" $
    node "S3" [| (uncurry (+)) |] $
    node "S4" [| (+2) |] $
    node "S5" [| (+2) |] $
    node "S6" [| (\(x,y,z) -> x + y + z) |] $
    ["S3", "S4", "S5"] ~> "S6" $ def

{-
main = do
  res <- CS.withStore [absdir|/tmp/funflow|] $ \store -> do
     runSciFlow hook store 623 wf 2
  case res of
    Left err -> putStrLn $ "Something went wrong: " ++ show err
    Right x -> print x
    -}

config :: DrmaaConfig
config = DrmaaConfig
    { _queue_size = 2
    , _cmd = ("/home/kai/dev/SciFlow/tests/exec", [])
    , _address = (192, 168, 0, 1)
    , _port = 8888 }

main = withDrmaa config $ \d -> do
  res <- CS.withStore [absdir|/home/kai/dev/SciFlow/tests/db|] $ \store -> do
     runCoordinator d store wf 2
  case res of
    Left err -> putStrLn $ "Something went wrong: " ++ show err
    Right x -> print x

