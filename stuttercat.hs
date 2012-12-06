#!/usr/bin/env runhaskell
{-# LANGUAGE OverloadedStrings #-}

import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TVar
import           Control.Concurrent.STM.TChan
import           Control.Exception
import           Control.Monad
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString hiding (hPutStrLn, pack)
import qualified Data.ByteString.Char8 as ByteString
import           Data.Maybe
import           Data.Monoid
import           Data.Time
import           Data.Time.Clock ()
import           System.Exit
import           System.IO
import           System.Mem



main :: IO ()
main  = go 25000 stdin

go :: Int -> Handle -> IO ()
go t h = do (blocks, chunks) <- atomically ctx
            me <- myThreadId
            msg (ByteString.pack ("me: "<>show me))
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
  me <- myThreadId
  msg (ByteString.pack ("handoff: "<>show me))
  Control.Exception.catch (forever (step me)) skip
 where skip    = const (return ()) :: AsyncException -> IO ()
       step me = forkIO (work me) *> threadDelay micros
       work me = do t <- getCurrentTime
                    hPutStrLn stderr (show t)
                    kill <- atomically $ do
                              recs <- readAll from
                              let recs' = catMaybes recs
                                  done  = any isNothing recs
                              not (null recs') `when` writeTChan to recs'
                              done `when` writeTChan to []
                              return done
                    kill `when` do killThread me
                    performGC

send :: TChan [ByteString] -> IO ()
send i = do chunks <- atomically $ readTChan i
            mapM_ ByteString.putStr chunks
            hFlush stdout
            (chunks /= []) `when` send i


readAll :: TChan t -> STM [t]
readAll chan = do h <- tryReadTChan chan
                  case h of Just h  -> (h:) <$> readAll chan
                            Nothing -> return []


msg :: ByteString -> IO ()
msg  = ByteString.hPutStrLn stderr

