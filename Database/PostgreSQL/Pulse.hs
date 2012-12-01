#!/usr/bin/env runhaskell
{-# LANGUAGE OverloadedStrings
           , ScopedTypeVariables
  #-}
module Database.PostgreSQL.Pulse where

import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.STM (STM)
import qualified Control.Concurrent.STM as STM
import qualified Control.Concurrent.STM.TChan as STM
import           Control.Monad
import           System.IO

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString hiding (hPutStrLn)
import qualified Data.ByteString.Char8 as ByteString (lines, hPutStrLn)
import qualified Data.Csv as Cassava
import qualified Data.Csv.Incremental as Cassava
import           Data.Vector (Vector)
import qualified Data.Vector as Vector


main :: IO ()
main  = do (a :: STM.TChan ByteString)          <- STM.atomically $ STM.newTChan
           (b :: STM.TChan (Vector ByteString)) <- STM.atomically $ STM.newTChan
           (c :: STM.TChan [Vector ByteString]) <- STM.atomically $ STM.newTChan
           _ <- forkIO . forever $ getBlock a
           _ <- forkIO . forever . STM.atomically $ breakIntoRecords a b
           _ <- forkIO . forever $ send c
           forever $ delayedBulkTransfer 500000 b c

delayedBulkTransfer :: Int -> STM.TChan t -> STM.TChan [t] -> IO ()
delayedBulkTransfer micros from to = do
  (forkIO . STM.atomically) (STM.writeTChan to =<< readAll from)
  threadDelay micros

readAll :: STM.TChan t -> STM [t]
readAll chan = do h <- STM.tryReadTChan chan
                  case h of Just h  -> (h:) <$> readAll chan
                            Nothing -> return []

breakIntoRecords :: STM.TChan ByteString
                 -> STM.TChan (Vector ByteString) -> STM () 
breakIntoRecords i o = do
  records <- Vector.fromList . ByteString.lines <$> STM.readTChan i
  STM.writeTChan o records

getBlock :: STM.TChan ByteString -> IO ()
getBlock o = do
  bytes <- ByteString.hGet stdin (16)
  hPutStrLn stderr "Got a block."
  when (bytes /= "") $ STM.atomically (STM.writeTChan o bytes)

send :: STM.TChan [Vector ByteString] -> IO ()
send i = do
  vecs <- STM.atomically $ STM.readTChan i
  let lists = (Vector.toList <$> vecs)
  mapM_ (mapM_ (ByteString.hPutStrLn stdout)) lists
  hFlush stdout

