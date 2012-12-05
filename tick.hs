#!/usr/bin/env runhaskell

import           Control.Concurrent
import           Control.Monad
import           Data.Time
import           Data.Time.Clock ()

main :: IO ()
main  = go 100000

go :: Int -> IO ()
go micros = forever tick
 where tick = do _ <- forkIO $ do t <- getCurrentTime
                                  putStrLn (show t)
                 threadDelay micros

