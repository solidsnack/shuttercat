#!/usr/bin/env runhaskell
{-# LANGUAGE OverloadedStrings #-}

import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TVar
import           Control.Concurrent.STM.TChan
import           Control.Monad
import           Data.Maybe
import           Data.Monoid
import           Data.Word
import           System.IO
import           System.Exit

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString hiding (hPutStrLn, pack)
import qualified Data.ByteString.Char8 as ByteString
import           Data.Either


main :: IO ()
main  = go 250000 stdin

go :: Int -> Handle -> IO ()
go t h = do (blocks, chunks) <- atomically ctx
            a <- async $ recv h     blocks
            b <- async $ handoff t  blocks chunks
            c <- async $ send              chunks
            mapM_ wait [a, b, c]
            exitSuccess
 where ctx = (,) <$> newTChan <*> newTChan


recv :: Handle -> TChan (Maybe ByteString) -> IO ()
recv h o = do bytes <- ByteString.hGetSome h 16384
              ("" /= bytes) `when` atomically (writeTChan o (Just bytes))
              (eof, closed) <- (,) <$> hIsEOF h <*> hIsClosed h
              if eof || closed then atomically (writeTChan o Nothing)
                               else recv h o

handoff :: Int -> TChan (Maybe t) -> TChan [t] -> IO ()
handoff micros from to = do
  msg "In handoff..."
  recs <- atomically $ readAll from
  let recs' = catMaybes recs
  msg ("Read "<>(ByteString.pack . show) (length recs')<>" records.")
  _ <- forkIO . atomically $ do not (null recs')   `when` writeTChan to recs'
                                any isNothing recs `when` writeTChan to []
  msg "...deciding whether to handoff again."
  not (any isNothing recs) `when` do msg "Sleeping."
                                     threadDelay micros
                                     msg "Handing off."
                                     handoff micros from to

send :: TChan [ByteString] -> IO ()
send i = do msg "In send..."
            chunks <- atomically $ readTChan i
            mapM_ ByteString.putStr chunks
            hFlush stdout
            (chunks /= []) `when` send i


readAll :: TChan t -> STM [t]
readAll chan = do h <- tryReadTChan chan
                  case h of Just h  -> (h:) <$> readAll chan
                            Nothing -> return []


msg :: ByteString -> IO ()
msg  = ByteString.hPutStrLn stderr
