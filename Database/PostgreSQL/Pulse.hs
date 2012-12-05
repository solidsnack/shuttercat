#!/usr/bin/env runhaskell
{-# LANGUAGE OverloadedStrings
           , ScopedTypeVariables
           , NoMonomorphismRestriction
           , ViewPatterns
  #-}
module Database.PostgreSQL.Pulse where

import           Control.Applicative
import           Control.Arrow (first)
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
go t h = do (blocks, csv, chunks, fuzz) <- atomically ctx
            a <- async $ recv h         blocks
            b <- async $ segments fuzz  blocks csv
            c <- async $ handoff t             csv chunks
            d <- async $ send                      chunks
            mapM_ wait [a, b, c, d]
            exitSuccess
 where ctx = (,,,) <$> newTChan <*> newTChan <*> newTChan <*> newTVar ""


recv :: Handle -> TChan (Maybe ByteString) -> IO ()
recv h o = do bytes <- ByteString.hGetSome h 16384
              ("" /= bytes) `when` atomically (writeTChan o (Just bytes))
              (eof, closed) <- (,) <$> hIsEOF h <*> hIsClosed h
              if eof || closed then atomically (writeTChan o Nothing)
                               else recv h o


segments :: TVar ByteString -> TChan (Maybe ByteString)
                            -> TChan [ByteString] -> IO ()
segments fuzz blocks csv = do
  block <- atomically $ readTChan blocks
  atomically $ maybe (writeTChan csv []) process block
  isJust block `when` segments fuzz blocks csv
 where process b = do leftover <- readTVar fuzz
                      let (results, remainder) = segment (leftover <> b)
                          (_,  good)           = partitionEithers results
                      writeTChan csv good
                      writeTVar fuzz remainder

handoff :: Int -> TChan [t] -> TChan [[t]] -> IO ()
handoff micros from to = do
  recs <- atomically $ readAll from
  _ <- forkIO . atomically $ do not (null recs) `when` writeTChan to recs
                                any null recs   `when` writeTChan to []

  not (any null recs) `when` do threadDelay micros
                                handoff micros from to

send :: TChan [[ByteString]] -> IO ()
send i = do chunks <- atomically $ readTChan i
            (mapM_ . mapM_) ByteString.putStr chunks
            hFlush stdout
            (chunks /= []) `when` send i


readAll :: TChan t -> STM [t]
readAll chan = do h <- tryReadTChan chan
                  case h of Just h  -> (h:) <$> readAll chan
                            Nothing -> return []


msg :: ByteString -> IO ()
msg  = ByteString.hPutStrLn stderr


-- | Stateful CSV recognizer. Takes sequences of whole records from the input
--   'ByteString', skipping bad records, and returns any remainder.
segment :: ByteString -> ([Either ByteString ByteString], ByteString)
segment "" = ([], "")
segment b  = case step b of (Just res, tail) -> first (res:) $ segment tail 
                            (Nothing,  tail) -> ([], ByteString.copy tail)

-- | Skips over blank lines from the beginning of the string and, if a full
--   record (or a bad one, terminated by a newline) is available, parses it
--   out and returns it with the tail. If not, returns the input string.
step :: ByteString -> (Maybe (Either ByteString ByteString), ByteString)
step (ByteString.splitAt 1 -> ("\n",  t)) = step t
step (ByteString.splitAt 2 -> ("\r\n",t)) = step t
step b = case plain 0 b of Nothing                  -> (Nothing, b)
                           Just (n, ok) | ok        -> (Just (Right h), t)
                                        | otherwise -> (Just (Left h),  t)
                             where (h, t) = ByteString.splitAt n b

plain, quoted :: Int -> ByteString -> Maybe (Int, Bool)

plain  offset (ByteString.splitAt 1 -> ("\"",  t)) = quoted (offset+1) t
plain  offset (ByteString.splitAt 2 -> ("\r\n",_)) = Just   (offset+2, True)
plain  offset (ByteString.splitAt 1 -> ("\n",  _)) = Just   (offset+1, True)
plain  _      (ByteString.splitAt 1 -> ("\r", "")) = Nothing
plain  offset (ByteString.splitAt 1 -> ("\r",  _)) = Just   (offset+1, False)
plain  _      (ByteString.splitAt 1 -> (""  ,  _)) = Nothing
plain  offset (ByteString.splitAt 1 -> (_   ,  t)) = plain  (offset+1) t

quoted offset (ByteString.splitAt 2 -> ("\"\"",t)) = quoted (offset+2) t
quoted offset (ByteString.splitAt 1 -> ("\"",  t)) = plain  (offset+1) t
quoted _      (ByteString.splitAt 1 -> (""  ,  _)) = Nothing
quoted offset (ByteString.splitAt 1 -> (_   ,  t)) = quoted (offset+1) t

