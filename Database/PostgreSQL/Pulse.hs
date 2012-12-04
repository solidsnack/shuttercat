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
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TVar
import           Control.Concurrent.STM.TChan
import           Control.Monad
import           Data.Monoid
import           Data.Word
import           System.IO

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString hiding (hPutStrLn)
import qualified Data.ByteString.Char8 as ByteString
import           Data.Either


main :: IO ()
main  = go 500000 stdin

go :: Int -> Handle -> IO ()
go t h = do (blocks, junk, csv, chunks, fuzz) <- context
            forget . forever $ recv h         blocks
            forget . forever $ segments fuzz  blocks junk csv
            forget . forever $ timedMessages t       junk
            forget . forever $ timedHandoff t             csv chunks
            forever          $ send                           chunks

context :: IO ( TChan ByteString, TChan ByteString,
                TChan ByteString, TChan [ByteString], TVar ByteString )
context  = atomically $ do
  (,,,,) <$> newTChan <*> newTChan <*> newTChan <*> newTChan <*> newTVar ""


recv :: Handle -> TChan ByteString -> IO ()
recv h o = do bytes <- ByteString.hGetSome h 16384
              when (bytes /= "") $ do -- msg "Got a block."
                                      atomically (writeTChan o bytes)

segments :: TVar ByteString
         -> TChan ByteString -> TChan ByteString -> TChan ByteString -> IO ()
segments fuzz blocks junk csv = atomically $ do
  bytes <- readTChan blocks
  ("" /= bytes) `when` do
    leftover <- readTVar fuzz
    let (results, remainder) = segment (leftover <> bytes)
        (bad, good)          = partitionEithers results
    mapM_ (writeTChan csv)  good
    mapM_ (writeTChan junk) bad
    writeTVar fuzz remainder

timedMessages :: Int -> TChan ByteString -> IO ()
timedMessages micros junk = do
  forget (maybeList "Bad CSV:" =<< atomically (readAll junk))
  threadDelay micros

timedHandoff :: Int -> TChan t -> TChan [t] -> IO ()
timedHandoff micros from to = do
  forget $ atomically (writeNonEmpty to =<< readAll from)
  threadDelay micros

send :: TChan [ByteString] -> IO ()
send i = do chunks <- atomically $ readTChan i
            mapM_ ByteString.putStr chunks
            hFlush stdout


writeNonEmpty :: TChan [t] -> [t] -> STM ()
writeNonEmpty chan x = not (null x) `when` writeTChan chan x

readAll :: TChan t -> STM [t]
readAll chan = do h <- tryReadTChan chan
                  case h of Just h  -> (h:) <$> readAll chan
                            Nothing -> return []


msg :: ByteString -> IO ()
msg  = ByteString.hPutStrLn stderr

maybeList :: ByteString -> [ByteString] -> IO ()
maybeList _      [   ] = return ()
maybeList remark stuff = msg remark >> mapM_ msg stuff >> hFlush stderr

forget :: IO () -> IO ()
forget io = () <$ forkIO io


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

